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
import org.apache.rocketmq.client.exception.MQClientException;
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
                sourceConfiguration, context, Collections.emptySet());
    }

    public RocketMQSourceEnumerator(
            InnerConsumer consumer,
            MessageQueueOffsets startingMessageQueueOffsets,
            MessageQueueOffsets stoppingMessageQueueOffsets,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<RocketMQPartitionSplit> context,
            Set<MessageQueue> assignedPartitions) {

        this.consumer = consumer;
        this.startingMessageQueueOffsets = startingMessageQueueOffsets;
        this.stoppingMessageQueueOffsets = stoppingMessageQueueOffsets;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;
        this.discoveredPartitions = Sets.newHashSet(assignedPartitions);
        this.allocateStrategy = new AllocateMergeStrategy();
    }

    //
    //    this.discoveredPartitions = new HashSet<>();
    //    this.readerIdToSplitAssignments = new HashMap<>(currentSplitsAssignments);
    //    this.readerIdToSplitAssignments.forEach(
    //            (reader, splits) ->
    //                    splits.forEach(
    //                            s ->
    //                                    discoveredPartitions.add(
    //                                            new Tuple3<>(
    //                                                    s.getTopicName(),
    //                                                    s.getBrokerName(),
    //                                                    s.getPartitionId()))));
    //    this.pendingPartitionSplitAssignment = new HashMap<>();
    //    this.consumerOffsetMode = consumerOffsetMode;
    //    this.consumerOffsetTimestamp = consumerOffsetTimestamp;
    // }

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
                    this::initializeAllPartitionSplit,
                    this::handleSplitChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::initializeAllPartitionSplit, this::handleSplitChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the rocketmq source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<RocketMQPartitionSplit> splits, int subtaskId) {
        //handleSplitChangeLocal(splits);
        handleSplitChangeRemote();
    }

    @Override
    public void addReader(int subtaskId) {
        // LOG.debug(
        //        "Adding reader {} to RocketMQSourceEnumerator for consumer group {}.",
        //        subtaskId,
        //        consumerGroup);
        // assignPendingPartitionSplits();
        // if (boundedness == Boundedness.BOUNDED) {
        //    // for RocketMQ bounded source, send this signal to ensure the task can end after all
        //    // the
        //    // splits assigned are completed.
        //    context.signalNoMoreSplits(subtaskId);
        // }
    }

    @Override
    public RocketMQSourceEnumState snapshotState(long checkpointId) {
        //return new RocketMQSourceEnumState(readerIdToSplitAssignments);
        return null;
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.error("asd");
            }
        }
    }

    // ----------------- private methods -------------------

    private Set<MessageQueue> initializeAllPartitionSplit() {
        try {
            Set<MessageQueue> existQueueSet = Collections.unmodifiableSet(discoveredPartitions);
            Map<String, TopicRouteData> topicRouteDataMap =
                    consumer.getTopicRoute(sourceConfiguration.getTopicList()).get();
            Set<MessageQueue> currentQueueSet = topicRouteDataMap.entrySet().stream()
                    .flatMap(entry -> MQClientInstance.topicRouteData2TopicSubscribeInfo(
                                    entry.getKey(), entry.getValue()).stream())
                    .collect(Collectors.toSet());
            currentQueueSet.removeAll(existQueueSet);
            discoveredPartitions.addAll(currentQueueSet);
        } catch (Exception e) {
            LOG.error("Initialize partition split failed", e);
        }
        return discoveredPartitions;
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSplitChanges(Set<MessageQueue> partitionSplits, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle partition splits change due to ", t);
        }
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.warn("Partition has changed, but not enable partition discovery");
        }
        handleSplitChangeLocal(partitionSplits);
        handleSplitChangeRemote();
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSplitChangeLocal(Collection<MessageQueue> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (MessageQueue split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split, numReaders);
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }
        LOG.debug(
                "Assigned {} to {} readers of consumer group {}.",
                newPartitionSplits,
                numReaders,
                sourceConfiguration.getConsumerGroup());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSplitChangeRemote() {
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
                            .computeIfAbsent(readerOwner, r -> new ArrayList<>())
                            .addAll(newPartitionSplits);
                    // Clear the pending splits for the reader owner.
                    pendingPartitionSplitAssignment.remove(readerOwner);
                    // Sends NoMoreSplitsEvent to the readers if there is no more partition splits
                    // to be assigned.
                    // if (noMoreNewPartitionSplits) {
                    //    LOG.debug(
                    //            "No more RocketMQPartitionSplits to assign. Sending
                    // NoMoreSplitsEvent to the readers "
                    //                    + "in consumer group {}.",
                    //            consumerGroup);
                    //    context.signalNoMoreSplits(readerOwner);
                    // }
                });
    }

    private long getOffsetByMessageQueue(MessageQueue mq) throws MQClientException {
        // Long offset = offsetTable.get(mq);
        // if (offset == null) {
        //    if (startOffset > 0) {
        //        offset = startOffset;
        //    } else {
        //        switch (consumerOffsetMode) {
        //            case RocketMQConfig.CONSUMER_OFFSET_EARLIEST:
        //                consumer.seekToBegin(mq);
        //                return -1;
        //            case RocketMQConfig.CONSUMER_OFFSET_LATEST:
        //                consumer.seekToEnd(mq);
        //                return -1;
        //            case RocketMQConfig.CONSUMER_OFFSET_TIMESTAMP:
        //                offset = consumer.offsetForTimestamp(mq, consumerOffsetTimestamp);
        //                break;
        //            default:
        //                offset = consumer.committed(mq);
        //                if (offset < 0) {
        //                    throw new IllegalArgumentException(
        //                            "Unknown value for CONSUMER_OFFSET_RESET_TO.");
        //                }
        //        }
        //    }
        // }
        // offsetTable.put(mq, offset);
        // return offsetTable.get(mq);
        return 0L;
    }

    /**
     * Returns the index of the target subtask that a specific RocketMQ partition should be assigned
     * to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param messageQueue the rocketmq message queue
     * @param numReaders the total number of readers.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(MessageQueue messageQueue, int numReaders) {
        String topic = messageQueue.getTopic();
        String broker = messageQueue.getBrokerName();
        int partition = messageQueue.getQueueId();
        int startIndex = (((topic + "-" + broker).hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of RocketMQ partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + partition) % numReaders;
    }
}
