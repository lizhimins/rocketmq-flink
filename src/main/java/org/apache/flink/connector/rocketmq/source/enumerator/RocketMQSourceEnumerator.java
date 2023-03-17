/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source.enumerator;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.InnerConsumerImpl;
import org.apache.flink.connector.rocketmq.source.config.SourceConfiguration;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateMergeStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The enumerator class for RocketMQ source.
 */
@Internal
public class RocketMQSourceEnumerator
        implements SplitEnumerator<RocketMQSourceSplit, RocketMQSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceEnumerator.class);

    private final SplitEnumeratorContext<RocketMQSourceSplit> context;

    // Lazily instantiated or mutable fields.
    private InnerConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private final OffsetsSelector minOffsetsSelector;
    private final OffsetsSelector maxOffsetsSelector;
    private final AllocateStrategy allocateStrategy;
    private final SourceConfiguration sourceConfiguration;

    // The internal states of the enumerator.
    // his set is only accessed by the partition discovery callable in the callAsync() method.
    // The current assignment by reader id. Only accessed by the coordinator thread.
    // The discovered and initialized partition splits that are waiting for owner reader to be ready.
    private final Set<MessageQueue> allocatedSet;
    private final Map<Integer, Set<RocketMQSourceSplit>> currentSplitAssignmentMap;
    private final Map<Integer, Set<RocketMQSourceSplit>> pendingSplitAssignmentMap;

    public RocketMQSourceEnumerator(
            InnerConsumer consumer,
            OffsetsSelector minOffsetsSelector,
            OffsetsSelector maxOffsetsSelector,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<RocketMQSourceSplit> context) {

        this(consumer, minOffsetsSelector, maxOffsetsSelector,
                sourceConfiguration, context, new ConcurrentHashMap<>());
    }

    public RocketMQSourceEnumerator(
            InnerConsumer consumer,
            OffsetsSelector minOffsetsSelector,
            OffsetsSelector maxOffsetsSelector,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<RocketMQSourceSplit> context,
            Map<Integer, Set<RocketMQSourceSplit>> currentSplitAssignmentMap) {

        this.consumer = consumer;
        this.minOffsetsSelector = minOffsetsSelector;
        this.maxOffsetsSelector = maxOffsetsSelector;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;

        // set allocate by strategy name
        this.allocateStrategy = new AllocateMergeStrategy();

        // restore discover
        this.pendingSplitAssignmentMap = new ConcurrentHashMap<>();
        this.currentSplitAssignmentMap = currentSplitAssignmentMap;
        this.allocatedSet = this.calculatedAllocatedSet(currentSplitAssignmentMap);
    }

    @Override
    public void start() {
        try {
            consumer = new InnerConsumerImpl(sourceConfiguration);
            consumer.start();
        } catch (MQClientException e) {
            LOG.info("consumer in enumerator start failed", e);
            throw new RuntimeException("consumer in enumerator start failed", e);
        }

        String consumerGroupId = sourceConfiguration.getConsumerGroup();
        long partitionDiscoveryIntervalMs = sourceConfiguration.getPartitionDiscoveryIntervalMs();
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);

            context.callAsync(
                    this::initializeMessageQueueSplits,
                    this::handleMessageQueueChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);

            context.callAsync(
                    this::initializeMessageQueueSplits,
                    this::handleMessageQueueChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the rocketmq source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<RocketMQSourceSplit> splits, int subtaskId) {
        handleSplitChangesLocal(new PartitionSplitChange(Sets.newHashSet(splits)));

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            handleSplitChangesRemote(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to RocketMQSourceEnumerator for consumer group {}.",
                subtaskId,
                sourceConfiguration.getConsumerGroup());
        handleSplitChangesRemote(Collections.singleton(subtaskId));
        if (sourceConfiguration.getBoundedness() == Boundedness.BOUNDED) {
            // for RocketMQ bounded source,
            // send this signal to ensure the task can end after all the splits assigned are completed.
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public RocketMQSourceEnumState snapshotState(long checkpointId) {
        return new RocketMQSourceEnumState(currentSplitAssignmentMap);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.error("Shutdown internal consumer error", e);
            }
        }
    }

    // ----------------- private methods -------------------

    private Set<MessageQueue> calculatedAllocatedSet(
            Map<Integer, Set<RocketMQSourceSplit>> currentSplitAssignmentMap) {

        return currentSplitAssignmentMap.values().stream()
                .flatMap(splits -> splits.stream().map(split ->
                        new MessageQueue(split.getTopic(), split.getBrokerName(), split.getQueueId())))
                .collect(Collectors.toSet());
    }

    private Set<MessageQueue> initializeMessageQueueSplits() {
        return sourceConfiguration.getTopicSet().stream()
                .flatMap(topic -> {
                    try {
                        return consumer.fetchMessageQueues(topic).get().stream();
                    } catch (MQClientException | InterruptedException | ExecutionException e) {
                        LOG.warn("Initialize partition split failed, " +
                                "Consumer fetch topic route failed, topic={}", topic, e);
                    }
                    return Stream.empty();
                })
                .collect(Collectors.toSet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleMessageQueueChanges(Set<MessageQueue> latestPartitionSet, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle partition splits change due to ", t);
        }
        if (!sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.warn("Partition has changed, but not enable partition discovery");
        }
        final MessageQueueChange messageQueueChange = getPartitionChange(latestPartitionSet);
        if (messageQueueChange.isEmpty()) {
            return;
        }
        context.callAsync(
                () -> initializePartitionSplits(messageQueueChange),
                this::handleSplitChanges);
    }

    // This method should only be invoked in the coordinator executor thread.
    private PartitionSplitChange initializePartitionSplits(MessageQueueChange messageQueueChange) {
        Set<MessageQueue> increaseSet =
                Collections.unmodifiableSet(messageQueueChange.getIncreaseSet());

        OffsetsSelector.MessageQueueOffsetsRetriever offsetsRetriever =
                new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);
        Map<MessageQueue, Long> minOffsets = minOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);
        Map<MessageQueue, Long> maxOffsets = maxOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);

        Set<RocketMQSourceSplit> increaseSplitSet = increaseSet.stream().map(mq -> {
            Long startingOffset = minOffsets.get(mq);
            long stoppingOffset = maxOffsets.getOrDefault(mq, RocketMQSourceSplit.NO_STOPPING_OFFSET);
            return new RocketMQSourceSplit(mq, startingOffset, stoppingOffset);
        }).collect(Collectors.toSet());

        LOG.info("Initialize increase message queue offset, {}", increaseSet);
        return new PartitionSplitChange(increaseSplitSet, messageQueueChange.getDecreaseSet());
    }

    /**
     * Mark partition splits initialized by {@link
     * RocketMQSourceEnumerator#initializePartitionSplits(MessageQueueChange)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param partitionSplitChange Partition split changes
     * @param t                    Exception in worker thread
     */
    private void handleSplitChanges(PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (!sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.info("Partition discovery is disabled.");
        }
        handleSplitChangesLocal(partitionSplitChange);
        handleSplitChangesRemote(context.registeredReaders().keySet());
    }

    /**
     * Calculate new split assignment according allocate strategy
     */
    private void handleSplitChangesLocal(PartitionSplitChange partitionSplitChange) {
        Map<Integer, Set<RocketMQSourceSplit>> newAssignments = allocateStrategy.allocate(
                currentSplitAssignmentMap, partitionSplitChange, context.currentParallelism());
        pendingSplitAssignmentMap.putAll(newAssignments);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSplitChangesRemote(Set<Integer> pendingReaders) {

        Map<Integer, List<RocketMQSourceSplit>> changedAssignmentMap = new ConcurrentHashMap<>();

        for (Integer pendingReader : pendingReaders) {
            if (!context.registeredReaders().containsKey(pendingReader)) {
                throw new IllegalStateException(
                        String.format("Reader %d is not registered to source coordinator", pendingReader));
            }

            // Remove pending assignment for the reader
            final Set<RocketMQSourceSplit> pendingAssignmentForReader =
                    pendingSplitAssignmentMap.remove(pendingReader);

            // Put pending assignment into incremental assignment
            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                changedAssignmentMap.put(pendingReader, Lists.newArrayList(pendingAssignmentForReader));
            }
        }

        // No assignment is made
        if (changedAssignmentMap.isEmpty()) {
            return;
        }

        LOG.info("Enumerator assigning split(s) to readers {}",
                JSON.toJSONString(changedAssignmentMap, false));

        context.assignSplits(new SplitsAssignment<>(changedAssignmentMap));
        changedAssignmentMap.forEach(
                (readerOwner, newPartitionSplits) -> {
                    // Update the split assignment.
                    currentSplitAssignmentMap.put(readerOwner, Sets.newHashSet(newPartitionSplits));

                    // Sends NoMoreSplitsEvent to the readers
                    // if there is no more partition splits to be assigned.
                    if (!sourceConfiguration.isEnablePartitionDiscovery()
                            && sourceConfiguration.getBoundedness() == Boundedness.BOUNDED) {
                        LOG.info("No more rocketmq partition to assign. " +
                                        "Sending NoMoreSplitsEvent to the readers in consumer group {}.",
                                sourceConfiguration.getConsumerGroup());
                        context.signalNoMoreSplits(readerOwner);
                    }
                });

        this.allocatedSet.clear();
        this.allocatedSet.addAll(this.calculatedAllocatedSet(this.currentSplitAssignmentMap));
    }

    /**
     * A container class to hold the newly added partitions and removed partitions.
     */
    @VisibleForTesting
    public static class MessageQueueChange {
        private final Set<MessageQueue> increaseSet;
        private final Set<MessageQueue> decreaseSet;

        public MessageQueueChange(Set<MessageQueue> increaseSet, Set<MessageQueue> decreaseSet) {
            this.increaseSet = increaseSet;
            this.decreaseSet = decreaseSet;
        }

        public Set<MessageQueue> getIncreaseSet() {
            return increaseSet;
        }

        public Set<MessageQueue> getDecreaseSet() {
            return decreaseSet;
        }

        public boolean isEmpty() {
            return increaseSet.isEmpty() && decreaseSet.isEmpty();
        }
    }

    @VisibleForTesting
    public static class PartitionSplitChange {
        private final Set<RocketMQSourceSplit> increaseSplits;
        private final Set<MessageQueue> decreasePartitions;

        public PartitionSplitChange(Set<RocketMQSourceSplit> increaseSplits) {
            this.increaseSplits = increaseSplits;
            this.decreasePartitions = Collections.emptySet();
        }

        private PartitionSplitChange(
                Set<RocketMQSourceSplit> increaseSplits,
                Set<MessageQueue> decreasePartitions) {
            this.increaseSplits = Collections.unmodifiableSet(increaseSplits);
            this.decreasePartitions = Collections.unmodifiableSet(decreasePartitions);
        }

        public Set<RocketMQSourceSplit> getIncreaseSplits() {
            return increaseSplits;
        }

        public Set<MessageQueue> getDecreasePartitions() {
            return decreasePartitions;
        }
    }

    @VisibleForTesting
    private MessageQueueChange getPartitionChange(Set<MessageQueue> fetchedPartitions) {
        Set<MessageQueue> currentSet = Collections.unmodifiableSet(allocatedSet);
        Set<MessageQueue> increaseSet = Sets.difference(fetchedPartitions, currentSet);
        Set<MessageQueue> decreaseSet = Sets.difference(currentSet, fetchedPartitions);

        MessageQueueChange changeResult = new MessageQueueChange(increaseSet, decreaseSet);
        if (changeResult.isEmpty()) {
            // before message queue set is same as latest message queue set
            LOG.info("Service discovery for update topic route, current mq size: {}", currentSet.size());
        } else {
            LOG.info("Service discovery for update topic route. " +
                            "Got partitions change event, current: {}, increase: {}, decrease: {}",
                    currentSet, increaseSet, decreaseSet);
        }
        return changeResult;
    }
}
