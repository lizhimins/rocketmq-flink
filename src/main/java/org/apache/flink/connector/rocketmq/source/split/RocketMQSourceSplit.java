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
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Objects;

import static org.apache.flink.connector.rocketmq.source.util.UtilAll.SEPARATOR;

/**
 * A {@link SourceSplit} for a RocketMQ partition.
 */
public class RocketMQSourceSplit implements SourceSplit {

    // -1 means Long.MAX_VALUE
    public static final long NO_STOPPING_OFFSET = -1L;

    private final String topic;
    private final String brokerName;
    private final int queueId;
    private final long minOffset;
    private final long maxOffset;

    public RocketMQSourceSplit(MessageQueue messageQueue, long minOffset, long maxOffset) {
        this.topic = messageQueue.getTopic();
        this.brokerName = messageQueue.getBrokerName();
        this.queueId = messageQueue.getQueueId();
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
    }

    public RocketMQSourceSplit(String topic, String brokerName, int queueId, long minOffset, long maxOffset) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    @Override
    public String splitId() {
        return topic + SEPARATOR + brokerName + SEPARATOR + queueId;
    }

    @Override
    public String toString() {
        return String.format(
                "(Topic: %s, BrokerName: %s, QueueId: %d, MinOffset: %d, MaxOffset: %d)",
                topic, brokerName, queueId, minOffset, maxOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, brokerName, queueId, minOffset, maxOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RocketMQSourceSplit)) {
            return false;
        }
        RocketMQSourceSplit other = (RocketMQSourceSplit) obj;
        return topic.equals(other.topic)
                && brokerName.equals(other.brokerName)
                && queueId == other.queueId
                && minOffset == other.minOffset
                && maxOffset == other.maxOffset;
    }
}
