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

package org.apache.flink.connector.rocketmq.source.enumerator.initializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * An interface for users to specify the starting / stopping offset of a {@link
 * RocketMQPartitionSplit}.
 *
 * @see ReaderHandledMessageQueueOffsetsStrategy
 * @see SpecifiedMessageQueueOffsetsStrategy
 * @see TimestampOffsetsStrategy
 */
@PublicEvolving
public interface OffsetsStrategy extends Serializable {

    Map<MessageQueue, Long> getMessageQueueOffsets(
            Collection<MessageQueue> partitions,
            MessageQueueOffsetsRetriever messageQueueOffsetsRetriever);

    OffsetResetStrategy getAutoOffsetResetStrategy();

    /**
     * An interface that provides necessary information to the {@link OffsetsStrategy} to get
     * the initial offsets of the RocketMQ message queues.
     */
    interface MessageQueueOffsetsRetriever {

        /**
         * The group id should be the set for {@link RocketMQSource } before invoking this method.
         * Otherwise, an {@code IllegalStateException} will be thrown.
         */
        Map<MessageQueue, Long> committedOffsets(Collection<MessageQueue> messageQueues);

        /**
         * List min offsets for the specified MessageQueues.
         */
        Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues);

        /**
         * List max offsets for the specified MessageQueues.
         */
        Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues);

        /**
         * List max offsets for the specified MessageQueues.
         */
        Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap);
    }

    // --------------- factory methods ---------------

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the committed offsets. An
     * exception will be thrown at runtime if there is no committed offsets.
     *
     * @return an offset initializer which initialize the offsets to the committed offsets.
     */
    static OffsetsStrategy committedOffsets() {
        return committedOffsets(OffsetResetStrategy.LATEST);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the committed offsets.
     * Use the given {@link OffsetResetStrategy} to initialize the offsets if the committed offsets
     * does not exist.
     *
     * @param offsetResetStrategy the offset reset strategy to use when the committed offsets do not
     *                            exist.
     * @return an {@link OffsetsStrategy} which initializes the offsets to the committed
     * offsets.
     */
    static OffsetsStrategy committedOffsets(OffsetResetStrategy offsetResetStrategy) {
        return new ReaderHandledMessageQueueOffsetsStrategy(
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, offsetResetStrategy);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets in each partition so that
     * the
     * initialized offset is the offset of the first record whose record timestamp is greater than
     * or equals the give timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the consumption.
     * @return an {@link OffsetsStrategy} which initializes the offsets based on the given
     * timestamp.
     */
    static OffsetsStrategy timestamp(long timestamp) {
        return new TimestampOffsetsStrategy(timestamp);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the earliest available
     * offsets of each partition.
     *
     * @return an {@link OffsetsStrategy} which initializes the offsets to the earliest
     * available offsets.
     */
    static OffsetsStrategy earliest() {
        return new ReaderHandledMessageQueueOffsetsStrategy(
                ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, OffsetResetStrategy.EARLIEST);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the latest offsets of
     * each partition.
     *
     * @return an {@link OffsetsStrategy} which initializes the offsets to the latest offsets.
     */
    static OffsetsStrategy latest() {
        return new ReaderHandledMessageQueueOffsetsStrategy(
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, OffsetResetStrategy.LATEST);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the specified offsets.
     *
     * @param offsets the specified offsets for each partition.
     * @return an {@link OffsetsStrategy} which initializes the offsets to the specified
     * offsets.
     */
    static OffsetsStrategy offsets(Map<MessageQueue, Long> offsets) {
        return new SpecifiedMessageQueueOffsetsStrategy(offsets, OffsetResetStrategy.EARLIEST);
    }

    /**
     * Get an {@link OffsetsStrategy} which initializes the offsets to the specified offsets.
     * Use the given {@link OffsetResetStrategy} to initialize the offsets in case the specified
     * offset is out of range.
     *
     * @param offsets             the specified offsets for each partition.
     * @param offsetResetStrategy the {@link OffsetResetStrategy} to use when the specified offset
     *                            is out of range.
     * @return an {@link OffsetsStrategy} which initializes the offsets to the specified
     * offsets.
     */
    static OffsetsStrategy offsets(Map<MessageQueue, Long> offsets, OffsetResetStrategy offsetResetStrategy) {
        return new SpecifiedMessageQueueOffsetsStrategy(offsets, offsetResetStrategy);
    }
}
