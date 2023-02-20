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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;

/**
 * Includes config options of RocketMQ connector type.
 */
public class RocketMQSourceOptions extends RocketMQOptions {

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic").stringType().noDefaultValue()
                    .withDescription("The name of the subscribe topic");

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("consumerGroup").stringType().noDefaultValue()
                    .withDescription("The name of the consumer group, used to identify a type of consumer");

    public static final ConfigOption<Boolean> OPTIONAL_USE_NEW_API =
            ConfigOptions.key("useNewApi").booleanType().defaultValue(true);

    /**
     * for message filter, rocketmq only support single filter option, so we can choose either tag or sql
     */
    public static final ConfigOption<String> OPTIONAL_TAG =
            ConfigOptions.key("tag").stringType().defaultValue("*");

    public static final ConfigOption<String> OPTIONAL_SQL =
            ConfigOptions.key("sql").stringType().noDefaultValue();

    /**
     * for initialization consume offset
     */
    public static final ConfigOption<Long> OPTIONAL_START_MESSAGE_OFFSET =
            ConfigOptions.key("startConsumeOffset")
                    .longType()
                    .defaultValue(RocketMQConfig.DEFAULT_START_MESSAGE_OFFSET);

    public static final ConfigOption<String> OPTIONAL_SCAN_STARTUP_MODE =
            ConfigOptions.key("scanStartupMode").stringType().defaultValue("latest");

    public static final ConfigOption<Long> OPTIONAL_OFFSET_FROM_TIMESTAMP =
            ConfigOptions.key("startConsumeOffset").longType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_START_TIME =
            ConfigOptions.key("startConsumeTime").stringType().noDefaultValue();

    public static final ConfigOption<Long> OPTIONAL_START_TIME_MILLS =
            ConfigOptions.key("startConsumeTimeMs").longType().defaultValue(-1L);

    public static final ConfigOption<String> OPTIONAL_STOP_TIME =
            ConfigOptions.key("stopConsumeTime").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_STOP_TIME_MILLS =
            ConfigOptions.key("stopConsumeTimeMs").stringType().noDefaultValue();

    /**
     * for message content check and split
     */
    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG =
            ConfigOptions.key("columnErrorDebug").booleanType().defaultValue(true);

    /**
     * pull config
     */
    public static final ConfigOption<Long> MESSAGE_MODEL =
            ConfigOptions.key("messageModel")
                    .longType().defaultValue(1000L)
                    .withDescription("The consumption mode");

    public static final ConfigOption<String> ALLOCATE_MESSAGE_QUEUE_STRATEGY =
            ConfigOptions.key("allocateMessageQueueStrategy")
                    .stringType().defaultValue(new AllocateMessageQueueAveragely().getName())
                    .withDescription("The load balancing strategy algorithm");

    public static final ConfigOption<Integer> PULL_THREAD_NUMS =
            ConfigOptions.key("pullThreadNums")
                    .intType().defaultValue(20)
                    .withDescription("The number of pull threads set");

    public static final ConfigOption<Long> PULL_BATCH_SIZE =
            ConfigOptions.key("pullBatchSize").longType().defaultValue(32L)
                    .withDescription("The maximum number of messages pulled each time");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_ALL =
            ConfigOptions.key("pullThresholdForAll").longType().defaultValue(10 * 1000L)
                    .withDescription("The threshold for flow control of consumed requests");

    public static final ConfigOption<Long> CONSUME_MAX_SPAN =
            ConfigOptions.key("consumeMaxSpan").longType().defaultValue(2 * 1000L)
                    .withDescription("The maximum offset span for consumption");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_QUEUE =
            ConfigOptions.key("pullThresholdForQueue").longType().defaultValue(1000L)
                    .withDescription("The queue level flow control threshold");

    public static final ConfigOption<Long> PULL_THRESHOLD_SIZE_FOR_QUEUE =
            ConfigOptions.key("pullThresholdSizeForQueue").longType().defaultValue(100 * 1024L * 1024L)
                    .withDescription("The queue level limit on cached message size");
    public static final ConfigOption<Long> POLL_TIMEOUT_MILLIS =
            ConfigOptions.key("pollTimeoutMillis").longType().defaultValue(5 * 1000L)
                    .withDescription("The polling timeout setting");

    public static final ConfigOption<Long> PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION =
            ConfigOptions.key("brokerSuspendMaxTimeMillis")
                    .longType().defaultValue(20 * 1000L)
                    .withDescription("The maximum time that a connection will be suspended " +
                            "for in long polling by the broker");

    public static final ConfigOption<Long> CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND =
            ConfigOptions.key("consumerTimeoutMillisWhenSuspend")
                    .longType().defaultValue(30 * 1000L)
                    .withDescription("The maximum wait time for a response from the broker " +
                            "in long polling by the client");

    public static final ConfigOption<Long> CONSUMER_PULL_TIMEOUT_MILLIS =
            ConfigOptions.key("consumerPullTimeoutMillis")
                    .longType().defaultValue(10 * 1000L)
                    .withDescription("The socket timeout for pulling messages");

    /**
     * for auto commit offset to rocketmq server
     */
    public static final ConfigOption<Boolean> AUTO_COMMIT =
            ConfigOptions.key("autoCommit").booleanType().defaultValue(true)
                    .withDescription("The setting for automatic commit of offset");

    public static final ConfigOption<Long> AUTO_COMMIT_INTERVAL_MILLIS =
            ConfigOptions.key("autoCommitIntervalMillis").longType().defaultValue(5 * 1000L)
                    .withDescription("Applies to Consumer, the interval for persisting consumption progress");

    /**
     * for message trace, suggest not enable when heavy traffic
     */
    public static final ConfigOption<Boolean> ENABLE_MESSAGE_TRACE =
            ConfigOptions.key("enableMsgTrace").booleanType().defaultValue(true)
                    .withDescription("The flag for message tracing");

    public static final ConfigOption<String> CUSTOMIZED_TRACE_TOPIC =
            ConfigOptions.key("customizedTraceTopic").stringType().noDefaultValue()
                    .withDescription("The name of the topic for message tracing");

}
