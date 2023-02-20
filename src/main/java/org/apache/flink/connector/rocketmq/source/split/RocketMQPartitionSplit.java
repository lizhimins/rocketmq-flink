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

package org.apache.flink.connector.rocketmq.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Objects;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQPartitionSplit implements SourceSplit {

    private final String topicName;
    private final String brokerName;
    private final int partition;
    private final long startingOffset;
    private final long stoppingTimestamp;

    public RocketMQPartitionSplit(
            String topicName,
            String broker,
            int partition,
            long startingOffset,
            long stoppingTimestamp) {
        this.topicName = topicName;
        this.brokerName = broker;
        this.partition = partition;
        this.startingOffset = startingOffset;
        this.stoppingTimestamp = stoppingTimestamp;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public int getPartition() {
        return partition;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getStoppingTimestamp() {
        return stoppingTimestamp;
    }

    @Override
    public String splitId() {
        return topicName + "-" + brokerName + "-" + partition;
    }

    @Override
    public String toString() {
        return String.format(
                "[Topic: %s, Broker: %s, Partition: %s, StartingOffset: %d, StoppingTimestamp: %d]",
                topicName, brokerName, partition, startingOffset, stoppingTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, brokerName, partition, startingOffset, stoppingTimestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RocketMQPartitionSplit)) {
            return false;
        }
        RocketMQPartitionSplit other = (RocketMQPartitionSplit) obj;
        return topicName.equals(other.topicName)
                && brokerName.equals(other.brokerName)
                && partition == other.partition
                && startingOffset == other.startingOffset
                && stoppingTimestamp == other.stoppingTimestamp;
    }

    public static String toSplitId(Tuple3<String, String, Integer> topicPartition) {
        return topicPartition.f0 + "-" + topicPartition.f1 + "-" + topicPartition.f2;
    }
}
