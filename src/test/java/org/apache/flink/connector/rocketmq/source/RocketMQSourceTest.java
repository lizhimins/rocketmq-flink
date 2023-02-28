package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class RocketMQSourceTest {

    @Test
    public void testBuilder() {
        RocketMQSource<String> source = RocketMQSource.<String>builder()
                .setEndpoints("localhost:9876")
                .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, "accessKey")
                .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, "secretKey")
                .setTopics("flink-topic-1", "flink-topic-2")
                .setTopics(Arrays.asList("flink-topic-1", "flink-topic-2"))
                .setGroupId("flink-source-group")

                .setStartingOffsets(MessageQueueOffsets.earliest())
                .setStartingOffsets(MessageQueueOffsets.latest())
                .setStartingOffsets(MessageQueueOffsets.committedOffsets())
                .setStartingOffsets(MessageQueueOffsets.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(MessageQueueOffsets.timestamp(System.currentTimeMillis()))


                .setConfig(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS, 10 * 1000L)
                .setDeserializer(new RocketMQDeserializationSchema<String>() {
                    @Override
                    public void deserialize(MessageView messageView, Collector<String> out) {
                        out.collect(new String(messageView.getBody()));
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })

                .setBodyOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return null;
                    }
                })
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "RocketMQ-Source");
    }
}
