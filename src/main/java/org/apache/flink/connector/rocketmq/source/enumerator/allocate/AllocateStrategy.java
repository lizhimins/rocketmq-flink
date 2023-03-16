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

import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumerator;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.Set;

/**
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateStrategy {

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getStrategyName();

    /**
     * Allocating by index and current parallelism
     *
     * @param currentAssignmentMap current assign message queue to reader map
     * @param partitionSplitChange partition change info
     * @param parallelism          the number of reader
     * @return latest assign message queue to reader map
     */
    Map<Integer, Set<RocketMQPartitionSplit>> allocate(
            final Map<Integer, Set<RocketMQPartitionSplit>> currentAssignmentMap,
            final RocketMQSourceEnumerator.PartitionSplitChange partitionSplitChange,
            final int parallelism);
}
