package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class RocketMQSourceTest {

    public static void main(String[] args) throws Exception {
        RocketMQSource<String> source = RocketMQSource.<String>builder()
                .setEndpoints("11.165.57.3:9876")
                .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, "accessKey")
                .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, "secretKey")
                .setGroupId("GID-flink")
                .setTopics("flink-topic-1", "flink-topic-2")
                .setStartingOffsets(MessageQueueOffsets.earliest())
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
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<String> dataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RocketMQ-Source").setParallelism(2);

        dataStream.print();

        env.execute("RocketMQ DataStream Job");
    }
}
