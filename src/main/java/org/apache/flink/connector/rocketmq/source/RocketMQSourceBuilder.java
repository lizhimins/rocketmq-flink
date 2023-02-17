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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfigBuilder;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@PublicEvolving
public class RocketMQSourceBuilder<OUT> {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceBuilder.class);

    // The subscriber specifies the partitions to subscribe to.
    protected DefaultLitePullConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private MessageQueueOffsets startingMessageQueueOffsets;
    private MessageQueueOffsets stoppingMessageQueueOffsets;

    // Boundedness
    private Boundedness boundedness;

    // Deserialization Schema
    private RocketMQDeserializationSchema<OUT> deserializationSchema;

    // The configurations.
    protected RocketMQConfigBuilder configBuilder;

    RocketMQSourceBuilder() {
    }

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public RocketMQSourceBuilder<OUT> setEndpoints(String endpoints) {
        consumer.setNamesrvAddr(endpoints);
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

    public RocketMQSourceBuilder<OUT> setStartingOffsets(MessageQueueOffsets startingMessageQueueOffsets) {
        this.startingMessageQueueOffsets = startingMessageQueueOffsets;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setUnbounded(MessageQueueOffsets stoppingMessageQueueOffsets) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stoppingMessageQueueOffsets = stoppingMessageQueueOffsets;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setBounded(MessageQueueOffsets stoppingMessageQueueOffsets) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingMessageQueueOffsets = stoppingMessageQueueOffsets;
        return this;
    }

    /**
     * now, rocketmq only support broadcast mode when broker version is v4
     * {@link MessageModel} is the consuming behavior for rocketmq, we would generate different
     * split by the given subscription type. Please take some time to consider which subscription
     * type matches your application best. Default is {@link SubscriptionType#Shared}.
     *
     * @param messageModel The type of subscription.
     * @return this PulsarSourceBuilder.
     * @see <a href="https://pulsar.apache.org/docs/en/concepts-messaging/#subscriptions">RocketMQ
     * Broadcast Subscriptions</a>
     */
    public RocketMQSourceBuilder<OUT> setMessageModel(MessageModel messageModel) {
        consumer.setMessageModel(messageModel);
        return this;
    }

    public RocketMQSourceBuilder<OUT> setDeserializer(
            RocketMQDeserializationSchema<OUT> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    public RocketMQSourceBuilder<OUT> setBodyOnlyDeserializer(
            DeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = RocketMQDeserializationSchema.flinkBodyOnlySchema(deserializationSchema);
        return this;
    }


    /**
     * Set an arbitrary property for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this PulsarSourceBuilder.
     */
    public <T> RocketMQSourceBuilder<OUT> setConfig(ConfigOption<T> key, T value) {
        configBuilder.set(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * @param config the config to set for the PulsarSource.
     * @return this PulsarSourceBuilder.
     */
    public RocketMQSourceBuilder<OUT> setConfig(Configuration config) {
        configBuilder.set(config);
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * <p>This method is mainly used for future flink SQL binding.
     *
     * @param properties the config properties to set for the PulsarSource.
     * @return this PulsarSourceBuilder.
     */
    public RocketMQSourceBuilder<OUT> setProperties(Properties properties) {
        configBuilder.set(properties);
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
        //return new RocketMQSource<>(
        //        startingMessageQueueOffsets,
        //        stoppingMessageQueueOffsets,
        //        boundedness,
        //        deserializationSchema,
        //        props);
        return null;
    }

    // ------------- private helpers  --------------

    /**
     * Helper method for java compiler recognize the generic type.
     */
    @SuppressWarnings("unchecked")
    private <T extends OUT> RocketMQSourceBuilder<T> specialized() {
        return (RocketMQSourceBuilder<T>) this;
    }

    private void ensureConsumerIsNull(String attemptingSubscribeMode) {
        if (consumer != null) {
            throw new IllegalStateException(
                    String.format("Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode, consumer.getClass().getSimpleName()));
        }
    }
}
