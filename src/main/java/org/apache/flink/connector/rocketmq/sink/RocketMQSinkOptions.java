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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;

public class RocketMQSinkOptions extends RocketMQOptions {

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic").stringType().noDefaultValue()
                    .withDescription("The name of the persist topic");

    public static final ConfigOption<String> TAG =
            ConfigOptions.key("tag").stringType().noDefaultValue()
                    .withDescription("The name of the persist topic");

    public static final ConfigOption<String> KEY =
            ConfigOptions.key("key").stringType().noDefaultValue()
                    .withDescription("The name of the persist topic");

    public static final ConfigOption<String> PRODUCER_GROUP =
            ConfigOptions.key("producerGroup").stringType().defaultValue("Flink-Producer");

    public static final ConfigOption<Integer> OPTIONAL_WRITE_RETRY_TIMES =
            ConfigOptions.key("retryTimes").intType().defaultValue(10);

    public static final ConfigOption<Long> OPTIONAL_WRITE_SLEEP_TIME_MS =
            ConfigOptions.key("sleepTimeMs").longType().defaultValue(5000L);

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_TAG =
            ConfigOptions.key("isDynamicTag").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN =
            ConfigOptions.key("dynamicTagColumn").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED =
            ConfigOptions.key("dynamicTagColumnWriteIncluded").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_WRITE_KEY_COLUMNS =
            ConfigOptions.key("keyColumns").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_KEYS_TO_BODY =
            ConfigOptions.key("writeKeysToBody").booleanType().defaultValue(false);
}
