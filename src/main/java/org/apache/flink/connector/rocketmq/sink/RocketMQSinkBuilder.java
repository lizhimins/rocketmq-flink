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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfigBuilder;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder to construct {@link RocketMQSink}.
 *
 * @see RocketMQSink for a more detailed explanation of the different guarantees.
 */
@PublicEvolving
public class RocketMQSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSinkBuilder.class);

    private RocketMQConfigBuilder configBuilder;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private RocketMQSerializationSchema<IN> serializer;

    RocketMQSinkBuilder() {
        this.configBuilder = configBuilder;
    }

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public RocketMQSinkBuilder<IN> setEndpoints(String endpoints) {
        return this.setConfig(RocketMQSinkOptions.ENDPOINTS, endpoints);
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
     * Set an arbitrary property for the RocketMQ source. The valid keys can be found in {@link RocketMQSourceOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key   the key of the property.
     * @param value the value of the property.
     * @return this RocketMQSourceBuilder.
     */
    public <T> RocketMQSinkBuilder<IN> setConfig(ConfigOption<T> key, T value) {
        configBuilder.set(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the RocketMQSink and RocketMQ Consumer.
     * The valid keys can be found in {@link RocketMQSinkOptions} and {@link RocketMQOptions}.
     *
     * @param config the config to set for the RocketMQSink.
     * @return this RocketMQSinkBuilder.
     */
    public RocketMQSinkBuilder<IN> setConfig(Configuration config) {
        configBuilder.set(config);
        return this;
    }

    /**
     * Set arbitrary properties for the RocketMQSink and RocketMQ Consumer.
     * The valid keys can be found in {@link RocketMQSinkOptions} and {@link RocketMQOptions}.
     *
     * @param properties the config properties to set for the RocketMQSink.
     * @return this RocketMQSinkBuilder.
     */
    public RocketMQSinkBuilder<IN> setProperties(Properties properties) {
        configBuilder.set(properties);
        return this;
    }

    /**
     * Sets the {@link RocketMQSerializationSchema} that transforms incoming records to {@link
     * org.apache.rocketmq.common.message.MessageExt}s.
     *
     * @param serializer serialize message
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setSerializer(
            RocketMQSerializationSchema<IN> serializer) {
        this.serializer = checkNotNull(serializer, "serializer is null");
        ClosureCleaner.clean(this.serializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    private void sanityCheck() {
    }

    /**
     * Build the {@link RocketMQSource}.
     *
     * @return a KafkaSource with the settings made for this builder.
     */
    public RocketMQSink<IN> build() {
        sanityCheck();
        return new RocketMQSink<>(configBuilder.build(null, null), serializer);
    }
}
