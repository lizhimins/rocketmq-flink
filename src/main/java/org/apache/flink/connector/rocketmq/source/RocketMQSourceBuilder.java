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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@PublicEvolving
public class RocketMQSourceBuilder<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceBuilder.class);

    // The subscriber specifies the partitions to subscribe to.
    // private KafkaSubscriber subscriber;

    // Users can specify the starting / stopping offset initializer.
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;

    // Boundedness
    private Boundedness boundedness;

    private RocketMQDeserializationSchema<OUT> deserializationSchema;

    // The configurations.
    protected Properties props;

    protected DefaultLitePullConsumer consumer;

    public RocketMQSourceBuilder() {}

    public RocketMQSourceBuilder<OUT> setNameServerAddr(String nameServerAddr) {
        consumer.setNamesrvAddr(nameServerAddr);
        return this;
    }

    /**
     * Sets the consumer group id of the KafkaSource.
     *
     * @param groupId the group id of the KafkaSource.
     * @return this KafkaSourceBuilder.
     */
    public RocketMQSourceBuilder<OUT> setGroupId(String groupId) {
        return this;
    }

    /**
     * Set a list of topics the KafkaSource should consume from. All the topics in the list should
     * have existed in the Kafka cluster. Otherwise, an exception will be thrown.
     *
     * @param topics the list of topics to consume from.
     * @return this KafkaSourceBuilder.
     */
    public RocketMQSourceBuilder<OUT> setTopics(List<String> topics) {
        return this;
    }

    /**
     * Set a list of topics the KafkaSource should consume from. All the topics in the list should
     * have existed in the Kafka cluster. Otherwise, an exception will be thrown.
     *
     * @param topics the list of topics to consume from.
     * @return this KafkaSourceBuilder.
     */
    public RocketMQSourceBuilder<OUT> setTopics(String... topics) {
        return this.setTopics(Arrays.asList(topics));
    }

    public RocketMQSourceBuilder<OUT> setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setUnbounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setDeserializer(
            RocketMQDeserializationSchema<OUT> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setValueOnlyDeserializer(
            RocketMQDeserializationSchema<OUT> deserializationSchema) {
        // this.deserializationSchema =
        //        RocketMQDeserializationSchema.valueOnly(deserializationSchema);
        return this;
    }

    public RocketMQSourceBuilder<OUT> setClientIdPrefix(String prefix) {
        // return setProperty(RocketMQOptions.CLIENT_ID_PREFIX.key(), prefix);
        return this;
    }

    public RocketMQSourceBuilder<OUT> setProperty(String key, String value) {
        props.setProperty(key, value);
        return this;
    }

    public RocketMQSourceBuilder<OUT> setProperties(Properties props) {
        this.props.putAll(props);
        return this;
    }

    /**
     * Build the {@link RocketMQSource}.
     *
     * @return a KafkaSource with the settings made for this builder.
     */
    public RocketMQSource<OUT> build() {
        // sanityCheck();
        // parseAndSetRequiredProperties();
        return new RocketMQSource<>(
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                deserializationSchema,
                props);
    }

    // ------------- private helpers  --------------

    // private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
    //    if (subscriber != null) {
    //        throw new IllegalStateException(
    //                String.format(
    //                        "Cannot use %s for consumption because a %s is already set for
    // consumption.",
    //                        attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
    //    }
    // }
    //
    // private void parseAndSetRequiredProperties() {
    //    maybeOverride(
    //            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    //            ByteArrayDeserializer.class.getName(),
    //            true);
    //    maybeOverride(
    //            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    //            ByteArrayDeserializer.class.getName(),
    //            true);
    //    if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
    //        LOG.warn(
    //                "Offset commit on checkpoint is disabled because {} is not specified",
    //                ConsumerConfig.GROUP_ID_CONFIG);
    //        maybeOverride(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false", false);
    //    }
    //    maybeOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false", false);
    //    maybeOverride(
    //            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
    //            startingOffsetsInitializer.getAutoOffsetResetStrategy().name().toLowerCase(),
    //            true);
    //
    //    // If the source is bounded, do not run periodic partition discovery.
    //    maybeOverride(
    //            KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
    //            "-1",
    //            boundedness == Boundedness.BOUNDED);
    //
    //    // If the client id prefix is not set, reuse the consumer group id as the client id
    // prefix,
    //    // or generate a random string if consumer group id is not specified.
    //    maybeOverride(
    //            KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
    //            props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)
    //                    ? props.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
    //                    : "KafkaSource-" + new Random().nextLong(),
    //            false);
    // }
    //
    // private boolean maybeOverride(String key, String value, boolean override) {
    //    boolean overridden = false;
    //    String userValue = props.getProperty(key);
    //    if (userValue != null) {
    //        if (override) {
    //            LOG.warn(
    //                    String.format(
    //                            "Property %s is provided but will be overridden from %s to %s",
    //                            key, userValue, value));
    //            props.setProperty(key, value);
    //            overridden = true;
    //        }
    //    } else {
    //        props.setProperty(key, value);
    //    }
    //    return overridden;
    // }
    //
    // private void sanityCheck() {
    //    // Check required configs.
    //    for (String requiredConfig : REQUIRED_CONFIGS) {
    //        checkNotNull(
    //                props.getProperty(requiredConfig),
    //                String.format("Property %s is required but not provided", requiredConfig));
    //    }
    //    // Check required settings.
    //    checkNotNull(
    //            subscriber,
    //            "No subscribe mode is specified, "
    //                    + "should be one of topics, topic pattern and partition set.");
    //    checkNotNull(deserializationSchema, "Deserialization schema is required but not
    // provided.");
    //    // Check consumer group ID
    //    checkState(
    //            props.containsKey(ConsumerConfig.GROUP_ID_CONFIG) ||
    // !offsetCommitEnabledManually(),
    //            String.format(
    //                    "Property %s is required when offset commit is enabled",
    //                    ConsumerConfig.GROUP_ID_CONFIG));
    //    // Check offsets initializers
    //    if (startingOffsetsInitializer instanceof OffsetsInitializerValidator) {
    //        ((OffsetsInitializerValidator) startingOffsetsInitializer).validate(props);
    //    }
    //    if (stoppingOffsetsInitializer instanceof OffsetsInitializerValidator) {
    //        ((OffsetsInitializerValidator) stoppingOffsetsInitializer).validate(props);
    //    }
    // }
    //
    // private boolean offsetCommitEnabledManually() {
    //    boolean autoCommit =
    //            props.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
    //                    && Boolean.parseBoolean(
    //                    props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    //    boolean commitOnCheckpoint =
    //            props.containsKey(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key())
    //                    && Boolean.parseBoolean(
    //                    props.getProperty(
    //                            KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key()));
    //    return autoCommit || commitOnCheckpoint;
    // }
}
