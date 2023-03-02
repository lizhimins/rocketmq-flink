/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumerator;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AllocateMergeStrategy implements AllocateStrategy {

    @Override
    public String getStrategyName() {
        return "merge";
    }

    @Override
    public Map<Integer, Set<RocketMQPartitionSplit>> allocate(
            Map<Integer, Set<RocketMQPartitionSplit>> currentAssignmentMap,
            RocketMQSourceEnumerator.PartitionSplitChange partitionSplitChange, int parallelism) {

        return null;
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
     * @param numReaders   the total number of readers.
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
