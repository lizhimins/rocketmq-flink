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

package org.apache.flink.connector.rocketmq.common.config;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** <a href="https://rocketmq.apache.org/zh/docs/4.x/parameterConfiguration/01local">...</a> */
public class RocketMQOptions {

    public static final ConfigOption<Boolean> GLOBAL_DEBUG_MODE =
            ConfigOptions.key("debug").booleanType().defaultValue(false);

    /**
     * rocketmq v4 endpoints means nameserver address rocketmq v5 endpoints means proxy server
     * address
     */
    public static final ConfigOption<String> ENDPOINTS =
            ConfigOptions.key("endpoints")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ server address");

    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RocketMQ instance namespace");

    /** 这里不知道对轨迹功能有没有影响, 待验证 */
    public static final ConfigOption<AccessChannel> OPTIONAL_ACCESS_CHANNEL =
            ConfigOptions.key("accessChannel")
                    .enumType(AccessChannel.class)
                    .defaultValue(AccessChannel.CLOUD)
                    .withDescription("RocketMQ access channel");

    public static final ConfigOption<Integer> CLIENT_CALLBACK_EXECUTOR_THREADS =
            ConfigOptions.key("clientCallbackExecutorThreads")
                    .intType()
                    .defaultValue(Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "The number of processor cores "
                                    + "when the client communication layer receives a network request");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key("partitionDiscoveryIntervalMs")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "Time interval for polling route information from nameserver or proxy");

    public static final ConfigOption<Long> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeatInterval")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Interval for regularly sending registration heartbeats to broker");

    public static final ConfigOption<Boolean> OPTIONAL_UNIT_MODE =
            ConfigOptions.key("unitMode").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_UNIT_NAME =
            ConfigOptions.key("unitName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> VIP_CHANNEL_ENABLED =
            ConfigOptions.key("vipChannelEnabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable vip netty channel for sending messages");

    public static final ConfigOption<Boolean> USE_TLS =
            ConfigOptions.key("useTLS")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS transport.");

    public static final ConfigOption<Long> MQ_CLIENT_API_TIMEOUT =
            ConfigOptions.key("mqClientApiTimeout")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("RocketMQ client api timeout setting");

    public static final ConfigOption<LanguageCode> LANGUAGE_CODE =
            ConfigOptions.key("language")
                    .enumType(LanguageCode.class)
                    .defaultValue(LanguageCode.JAVA)
                    .withDescription("Client implementation language");

    public static final ConfigOption<String> OPTIONAL_TIME_ZONE =
            ConfigOptions.key("timeZone").stringType().noDefaultValue();

    // for message content
    public static final ConfigOption<String> OPTIONAL_ENCODING =
            ConfigOptions.key("encoding").stringType().defaultValue("UTF-8");

    public static final ConfigOption<String> OPTIONAL_FIELD_DELIMITER =
            ConfigOptions.key("fieldDelimiter").stringType().defaultValue("\u0001");

    public static final ConfigOption<String> OPTIONAL_LINE_DELIMITER =
            ConfigOptions.key("lineDelimiter").stringType().defaultValue("\n");

    public static final ConfigOption<String> OPTIONAL_LENGTH_CHECK =
            ConfigOptions.key("lengthCheck").stringType().defaultValue("NONE");

    // the config of session credential
    public static final ConfigOption<String> OPTIONAL_ACCESS_KEY =
            ConfigOptions.key("accessKey").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SECRET_KEY =
            ConfigOptions.key("secretKey").stringType().noDefaultValue();
}
