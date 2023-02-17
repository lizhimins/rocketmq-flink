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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfigBuilder;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder to construct {@link KafkaSink}.
 *
 * <p>The following example shows the minimum setup to create a KafkaSink that writes String values
 * to a Kafka topic.
 *
 * <pre>{@code
 * KafkaSink<String> sink = KafkaSink
 *     .<String>builder
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setRecordSerializer(MY_RECORD_SERIALIZER)
 *     .build();
 * }</pre>
 *
 * <p>One can also configure different {@link DeliveryGuarantee} by using {@link
 * #setDeliverGuarantee(DeliveryGuarantee)} but keep in mind when using {@link
 * DeliveryGuarantee#EXACTLY_ONCE} one must set the transactionalIdPrefix {@link
 * #setTransactionalIdPrefix(String)}.
 *
 * @param <IN> type of the records written to Kafka
*             The builder class for {@link PulsarSink} to make it easier for the users to construct a {@link
*             PulsarSink}.
*
*             <p>The following example shows the minimum setup to create a PulsarSink that reads the String
*             values from a Pulsar topic.
*
*             <pre>{@code
*             PulsarSink<String> sink = PulsarSink.builder()
*                 .setServiceUrl(operator().serviceUrl())
*                 .setAdminUrl(operator().adminUrl())
*                 .setTopics(topic)
*                 .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.STRING))
*                 .build();
*             }</pre>
*
*             <p>The service url, admin url, and the record serializer are required fields that must be set. If
*             you don't set the topics, make sure you have provided a custom {@link TopicRouter}. Otherwise,
*             you must provide the topics to produce.
*
*             <p>To specify the delivery guarantees of PulsarSink, one can call {@link
*             #setDeliveryGuarantee(DeliveryGuarantee)}. The default value of the delivery guarantee is {@link
*             DeliveryGuarantee#NONE}, and it wouldn't promise the consistence when write the message into
*             Pulsar.
*
*             <pre>{@code
*             PulsarSink<String> sink = PulsarSink.builder()
*                 .setServiceUrl(operator().serviceUrl())
*                 .setAdminUrl(operator().adminUrl())
*                 .setTopics(topic)
*                 .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.STRING))
*                 .setDeliveryGuarantee(deliveryGuarantee)
*                 .build();
*             }</pre>
 * @param <IN> The input type of the sink.
 * @see KafkaSink for a more detailed explanation of the different guarantees.
 * @see RocketMQSink for a more detailed explanation of the different guarantees.
 */
@PublicEvolving
public class RocketMQSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSinkBuilder.class);

    private static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofHours(1);

    private RocketMQConfigBuilder configBuilder;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private RocketMQSerializationSchema<IN> serializer;

    RocketMQSinkBuilder() {
        //this.configBuilder = configBuilder;
    }

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public RocketMQSinkBuilder<IN> setEndpoints(String endpoints) {
        //consumer.setNamesrvAddr(endpoints);
        return this;
    }

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * @param config the config to set for the PulsarSource.
     * @return this PulsarSourceBuilder.
     */
    public RocketMQSinkBuilder<IN> setConfig(Configuration config) {
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
    public RocketMQSinkBuilder<IN> setProperties(Properties properties) {
        configBuilder.set(properties);
        return this;
    }

    /**
     * Sets the {@link KafkaRecordSerializationSchema} that transforms incoming records to {@link
     * org.apache.kafka.clients.producer.ProducerRecord}s.
     *
     * @param recordSerializer
     * @return {@link KafkaSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setRecordSerializer(
            RocketMQSerializationSchema<IN> recordSerializer) {
        this.serializer = checkNotNull(recordSerializer, "serializer is null");
        ClosureCleaner.clean(
                this.serializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Build the {@link RocketMQSource}.
     *
     * @return a KafkaSource with the settings made for this builder.
     */
    public RocketMQSink<IN> build() {
        // sanityCheck();
        // parseAndSetRequiredProperties();
        //return new RocketMQSource<>(
        //        startingOffsetsInitializer,
        //        stoppingOffsetsInitializer,
        //        boundedness,
        //        deserializationSchema,
        //        props);
        return null;
    }





















}
