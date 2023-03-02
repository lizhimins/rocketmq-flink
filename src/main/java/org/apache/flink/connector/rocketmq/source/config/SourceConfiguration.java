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

package org.apache.flink.connector.rocketmq.source.config;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfiguration;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;

import java.util.Arrays;
import java.util.List;

public class SourceConfiguration extends RocketMQConfiguration {

    private static final String TOPIC_LIST_SEPARATE = ";";

    private final List<String> topicList;

    private final String consumerGroup;

    private final long partitionDiscoveryIntervalMs;

    private final Boundedness boundedness;

    //private final int messageQueueCapacity;
    //private final long partitionDiscoveryIntervalMs;
    //private final boolean enableAutoAcknowledgeMessage;
    //private final long autoCommitCursorInterval;
    //private final long transactionTimeoutMillis;
    //private final Duration maxFetchTime;
    //private final int maxFetchRecords;
    //private final OffsetVerification verifyInitialOffsets;
    //private final String subscriptionName;
    //private final boolean allowKeySharedOutOfOrderDelivery;

    /**
     * Creates a new PulsarConfiguration, which holds a copy of the given configuration that can't
     * be altered.
     *
     * @param config The configuration with the original contents.
     */
    public SourceConfiguration(Configuration config) {
        super(config);

        this.topicList = Arrays.asList(config.getString(RocketMQSourceOptions.TOPIC).split(TOPIC_LIST_SEPARATE));
        this.consumerGroup = config.getString(RocketMQSourceOptions.CONSUMER_GROUP);
        this.partitionDiscoveryIntervalMs = config.getLong(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;

        //this.messageQueueCapacity = 1;
        //this.partitionDiscoveryIntervalMs = 1;
        //this.enableAutoAcknowledgeMessage = false;
        //this.autoCommitCursorInterval = 5000L;
        //this.transactionTimeoutMillis = 100L;
        //this.maxFetchTime = Duration.ofSeconds(1);
        //this.maxFetchRecords = 1000;
        //this.verifyInitialOffsets = null;
        //this.subscriptionName = null;
        //this.allowKeySharedOutOfOrderDelivery = false;
    }

    public List<String> getTopicList() {
        return topicList;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    /**
     * We would override the interval into a negative number when we set the connector with bounded
     * stop cursor.
     */
    public boolean isEnablePartitionDiscovery() {
        return getPartitionDiscoveryIntervalMs() > 0;
    }

    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    public Boundedness getBoundedness() {
        return boundedness;
    }
}
