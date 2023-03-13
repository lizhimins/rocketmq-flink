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
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The enumerator class for RocketMQ source.
 */
@Internal
public class RocketMQSourceEnumerator
        implements SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceEnumerator.class);

    // Lazily instantiated or mutable fields.
    private InnerConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private final MessageQueueOffsets startingMessageQueueOffsets;
    private final MessageQueueOffsets stoppingMessageQueueOffsets;

    private final SourceConfiguration sourceConfiguration;

    private final SplitEnumeratorContext<RocketMQPartitionSplit> context;

    private final AllocateStrategy allocateStrategy;

    // The internal states of the enumerator.
    // his set is only accessed by the partition discovery callable in the callAsync() method.
    private final Set<MessageQueue> discoveredPartitions;

    // The current assignment by reader id. Only accessed by the coordinator thread.
    private Map<Integer, Set<RocketMQPartitionSplit>> currentPartitionSplitAssignment;

    // The discovered and initialized partition splits that are waiting for owner reader to be ready.
    private Map<Integer, Set<RocketMQPartitionSplit>> pendingPartitionSplitAssignment;

    public RocketMQSourceEnumerator(
            InnerConsumer consumer,
            MessageQueueOffsets startingMessageQueueOffsets,
            MessageQueueOffsets stoppingMessageQueueOffsets,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<RocketMQPartitionSplit> context) {

        this(consumer, startingMessageQueueOffsets, stoppingMessageQueueOffsets,
                sourceConfiguration, context, Collections.emptyMap());
    }

    public RocketMQSourceEnumerator(
            InnerConsumer consumer,
            MessageQueueOffsets startingMessageQueueOffsets,
            MessageQueueOffsets stoppingMessageQueueOffsets,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<RocketMQPartitionSplit> context,
            Map<Integer, Set<RocketMQPartitionSplit>> currentPartitionSplitAssignment) {

        this.consumer = consumer;
        this.startingMessageQueueOffsets = startingMessageQueueOffsets;
        this.stoppingMessageQueueOffsets = stoppingMessageQueueOffsets;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;

        // set allocate by strategy name
        this.allocateStrategy = new AllocateMergeStrategy();

        // restore discover
        this.discoveredPartitions = currentPartitionSplitAssignment.values().stream()
                .flatMap(splits -> splits.stream().map(split ->
                        new MessageQueue(split.getTopicName(), split.getBrokerName(), split.getPartitionId())))
                .collect(Collectors.toSet());
        this.currentPartitionSplitAssignment = currentPartitionSplitAssignment;
    }

    @Override
    public void start() {
        consumer = new InnerConsumerImpl(sourceConfiguration);
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
    public void addSplitsBack(List<RocketMQPartitionSplit> splits, int subtaskId) {
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
        return new RocketMQSourceEnumState(currentPartitionSplitAssignment);
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

    private Set<MessageQueue> initializeMessageQueueSplits() {
        try {
            Set<MessageQueue> existQueueSet = Collections.unmodifiableSet(discoveredPartitions);
            Set<MessageQueue> currentQueueSet = Sets.newHashSet();
            for (String topic : sourceConfiguration.getTopicSet()) {
                currentQueueSet.addAll(consumer.fetchMessageQueues(topic).get());
            }
            currentQueueSet.removeAll(existQueueSet);
            discoveredPartitions.addAll(currentQueueSet);
        } catch (Exception e) {
            LOG.error("Initialize partition split failed", e);
        }
        return discoveredPartitions;
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleMessageQueueChanges(Set<MessageQueue> latestPartitionSet, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle partition splits change due to ", t);
        }
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
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
        Set<MessageQueue> newPartitions =
                Collections.unmodifiableSet(messageQueueChange.getNewPartitions());

        MessageQueueOffsets.MessageQueueOffsetsRetriever offsetsRetriever =
                new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);
        Map<MessageQueue, Long> startingOffsets =
                startingMessageQueueOffsets.getMessageQueueOffsets(newPartitions, offsetsRetriever);
        Map<MessageQueue, Long> stoppingOffsets =
                stoppingMessageQueueOffsets.getMessageQueueOffsets(newPartitions, offsetsRetriever);

        Set<RocketMQPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (MessageQueue processQueue : newPartitions) {
            Long startingOffset = startingOffsets.get(processQueue);
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(processQueue, RocketMQPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new RocketMQPartitionSplit(processQueue, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, messageQueueChange.getRemovedPartitions());
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
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.info("Partition discovery is disabled.");
        }
        handleSplitChangesLocal(partitionSplitChange);
        handleSplitChangesRemote(context.registeredReaders().keySet());
    }

    private void handleSplitChangesLocal(PartitionSplitChange partitionSplitChange) {
        int numReaders = context.currentParallelism();
        Map<Integer, Set<RocketMQPartitionSplit>> newPartitionSplits =
                allocateStrategy.allocate(currentPartitionSplitAssignment, partitionSplitChange, numReaders);
        pendingPartitionSplitAssignment.putAll(newPartitionSplits);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSplitChangesRemote(Set<Integer> pendingReaders) {
        Map<Integer, List<RocketMQPartitionSplit>> incrementalAssignment = new HashMap<>();
        pendingPartitionSplitAssignment.forEach(
                (ownerReader, pendingSplits) -> {
                    if (!pendingSplits.isEmpty()
                            && context.registeredReaders().containsKey(ownerReader)) {
                        // The owner reader is ready, assign the split to the owner reader.
                        incrementalAssignment
                                .computeIfAbsent(ownerReader, r -> new ArrayList<>())
                                .addAll(pendingSplits);
                    }
                });
        if (incrementalAssignment.isEmpty()) {
            // No assignment is made.
            return;
        }

        LOG.info("Assigning splits to readers {}", incrementalAssignment);
        context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        incrementalAssignment.forEach(
                (readerOwner, newPartitionSplits) -> {
                    // Update the split assignment.
                    currentPartitionSplitAssignment
                            .computeIfAbsent(readerOwner, r -> new HashSet<>())
                            .addAll(newPartitionSplits);
                    // Clear the pending splits for the reader owner.
                    pendingPartitionSplitAssignment.remove(readerOwner);
                    // Sends NoMoreSplitsEvent to the readers if there is no more partition splits
                    // to be assigned.
                    if (!sourceConfiguration.isEnablePartitionDiscovery()
                            && sourceConfiguration.getBoundedness() == Boundedness.BOUNDED) {
                        LOG.debug(
                                "No more RocketMQPartitionSplits to assign. Sending NoMoreSplitsEvent to the readers "
                                        + "in consumer group {}.",
                                sourceConfiguration.getConsumerGroup());
                        context.signalNoMoreSplits(readerOwner);
                    }
                });
    }

    /**
     * A container class to hold the newly added partitions and removed partitions.
     */
    @VisibleForTesting
    public static class MessageQueueChange {
        private final Set<MessageQueue> newPartitions;
        private final Set<MessageQueue> removedPartitions;

        MessageQueueChange(Set<MessageQueue> newPartitions, Set<MessageQueue> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<MessageQueue> getNewPartitions() {
            return newPartitions;
        }

        public Set<MessageQueue> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    @VisibleForTesting
    public static class PartitionSplitChange {
        private final Set<RocketMQPartitionSplit> newPartitionSplits;
        private final Set<MessageQueue> removedPartitions;

        public PartitionSplitChange(Set<RocketMQPartitionSplit> newPartitionSplits) {
            this.newPartitionSplits = newPartitionSplits;
            this.removedPartitions = Collections.emptySet();
        }

        private PartitionSplitChange(
                Set<RocketMQPartitionSplit> newPartitionSplits,
                Set<MessageQueue> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }
    }

    @VisibleForTesting
    MessageQueueChange getPartitionChange(Set<MessageQueue> latestMessageQueueSet) {
        Set<MessageQueue> beforeSet = Collections.unmodifiableSet(discoveredPartitions);
        Set<MessageQueue> newPartitions = Sets.difference(beforeSet, latestMessageQueueSet);
        Set<MessageQueue> removedPartitions = Sets.difference(latestMessageQueueSet, beforeSet);

        MessageQueueChange change = new MessageQueueChange(newPartitions, removedPartitions);
        if (!newPartitions.isEmpty() || !removedPartitions.isEmpty()) {
            LOG.info("Got partitions change event, before: {}, total: {}, new: {}, removed: {}",
                    beforeSet, latestMessageQueueSet, change.getNewPartitions(), change.getRemovedPartitions());
        } else {
            // before message queue set is same as latest message queue set
            LOG.info("No partitions change, total: {}", beforeSet);
        }
        return change;
    }
}
