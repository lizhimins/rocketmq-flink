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

package org.apache.flink.connector.rocketmq.sink.config;

import org.apache.flink.annotation.Internal;

import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Create the {@link Producer} to send message and a validator for building sink config.
 **/
@Internal
public class RocketMQSinkConfigUtils {

    private RocketMQSinkConfigUtils() {
        // No need to create instance.
    }

    //public static final PulsarConfigValidator SINK_CONFIG_VALIDATOR =
    //        PulsarConfigValidator.builder()
    //                .requiredOption(PULSAR_SERVICE_URL)
    //                .requiredOption(PULSAR_ADMIN_URL)
    //                .conflictOptions(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP)
    //                .conflictOptions(PULSAR_MEMORY_LIMIT_BYTES, PULSAR_MAX_PENDING_MESSAGES)
    //                .conflictOptions(
    //                        PULSAR_MEMORY_LIMIT_BYTES,
    //                        PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS)
    //                .build();
    //
    ///** Create a pulsar producer builder by using the given Configuration. */
    //public static <T> ProducerBuilder<T> createProducerBuilder(
    //        PulsarClient client, Schema<T> schema, SinkConfiguration configuration) {
    //    ProducerBuilder<T> builder = client.newProducer(schema);
    //
    //    configuration.useOption(
    //            PULSAR_PRODUCER_NAME,
    //            producerName -> String.format(producerName, UUID.randomUUID()),
    //            builder::producerName);
    //    configuration.useOption(
    //            PULSAR_SEND_TIMEOUT_MS,
    //            Math::toIntExact,
    //            ms -> builder.sendTimeout(ms, MILLISECONDS));
    //    configuration.useOption(PULSAR_MAX_PENDING_MESSAGES, builder::maxPendingMessages);
    //    configuration.useOption(
    //            PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS,
    //            builder::maxPendingMessagesAcrossPartitions);
    //    configuration.useOption(
    //            PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS,
    //            s -> builder.batchingMaxPublishDelay(s, MICROSECONDS));
    //    configuration.useOption(
    //            PULSAR_BATCHING_PARTITION_SWITCH_FREQUENCY_BY_PUBLISH_DELAY,
    //            builder::roundRobinRouterBatchingPartitionSwitchFrequency);
    //    configuration.useOption(PULSAR_BATCHING_MAX_MESSAGES, builder::batchingMaxMessages);
    //    configuration.useOption(PULSAR_BATCHING_MAX_BYTES, builder::batchingMaxBytes);
    //    configuration.useOption(PULSAR_BATCHING_ENABLED, builder::enableBatching);
    //    configuration.useOption(PULSAR_CHUNKING_ENABLED, builder::enableChunking);
    //    configuration.useOption(PULSAR_COMPRESSION_TYPE, builder::compressionType);
    //    configuration.useOption(PULSAR_INITIAL_SEQUENCE_ID, builder::initialSequenceId);
    //
    //    // Set producer properties
    //    Map<String, String> properties = configuration.getProperties(PULSAR_PRODUCER_PROPERTIES);
    //    if (!properties.isEmpty()) {
    //        builder.properties(properties);
    //    }
    //
    //    // Set the default value for current producer builder.
    //    // We use non-partitioned producer by default. This wouldn't be changed in the future.
    //    builder.blockIfQueueFull(true)
    //            .messageRoutingMode(SinglePartition)
    //            .enableMultiSchema(false)
    //            .autoUpdatePartitions(false)
    //            .accessMode(Shared)
    //            .enableLazyStartPartitionedProducers(false);
    //
    //    return builder;
    //}
}
