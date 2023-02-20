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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfiguration;

import java.time.Duration;

public class SourceConfiguration extends RocketMQConfiguration {

    private final int messageQueueCapacity;
    private final long partitionDiscoveryIntervalMs;
    private final boolean enableAutoAcknowledgeMessage;
    private final long autoCommitCursorInterval;
    private final long transactionTimeoutMillis;
    private final Duration maxFetchTime;
    private final int maxFetchRecords;
    private final OffsetVerification verifyInitialOffsets;
    private final String subscriptionName;
    // private final SubscriptionType subscriptionType;
    // private final SubscriptionMode subscriptionMode;
    private final boolean allowKeySharedOutOfOrderDelivery;

    /**
     * Creates a new PulsarConfiguration, which holds a copy of the given configuration that can't
     * be altered.
     *
     * @param config The configuration with the original contents.
     */
    public SourceConfiguration(Configuration config) {
        super(config);

        this.messageQueueCapacity = 1;
        this.partitionDiscoveryIntervalMs = 1;
        this.enableAutoAcknowledgeMessage = false;
        this.autoCommitCursorInterval = 5000L;
        this.transactionTimeoutMillis = 100L;
        this.maxFetchTime = Duration.ofSeconds(1);
        this.maxFetchRecords = 1000;
        this.verifyInitialOffsets = null;
        this.subscriptionName = null;
        this.allowKeySharedOutOfOrderDelivery = false;
    }

    // private final String consumerOffsetMode;
    // private final long consumerOffsetTimestamp;
    //
    /// ** The topic used for this RocketMQSource. */
    // private final String topic;
    /// ** The consumer group used for this RocketMQSource. */
    // private final String consumerGroup;
    /// ** The name server address used for this RocketMQSource. */
    // private final String nameServerAddress;
    /// ** The stop timestamp for this RocketMQSource. */
    //
    // private final long stopInMs;
    /// ** The start offset for this RocketMQSource. */
    // private final long startOffset;
    /// ** The partition discovery interval for this RocketMQSource. */
    // private final long partitionDiscoveryIntervalMs;
    /// ** The boundedness of this RocketMQSource. */
    // private final Boundedness boundedness;

    /// ** The accessKey used for this RocketMQSource. */
    // private final String accessKey;
    /// ** The secretKey used for this RocketMQSource. */
    // private final String secretKey;

}
