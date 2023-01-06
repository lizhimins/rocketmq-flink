package org.apache.flink.connector.rocketmq.source.enumerator.initializer;

import org.apache.flink.annotation.Internal;

import java.util.Properties;

/**
 * Interface for validating {@link OffsetsInitializer} with properties from {@link
 * org.apache.flink.connector.kafka.source.KafkaSource}.
 */
@Internal
public interface OffsetsInitializerValidator {

    /**
     * Validate offsets initializer with properties of Kafka source.
     *
     * @param kafkaSourceProperties Properties of Kafka source
     * @throws IllegalStateException if validation fails
     */
    void validate(Properties kafkaSourceProperties) throws IllegalStateException;
}
