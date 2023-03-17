package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RocketMQSourceTest {

    public static void main(String[] args) throws Exception {

        RocketMQSource<String> source = RocketMQSource.<String>builder()
                .setEndpoints("11.165.57.3:9876")
                .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, "accessKey")
                .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, "secretKey")
                .setGroupId("GID-flink")
                .setTopics("flink-source-1", "flink-source-2")
                .setMinOffsets(OffsetsSelector.earliest())
                .setDeserializer(new RocketMQDeserializationSchema<String>() {
                    @Override
                    public void deserialize(MessageView messageView, Collector<String> out) {
                        System.out.println(messageView.getMessageId());
                        out.collect(messageView.getMessageId());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStreamSource<String> dataStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "rocketmq-source").setParallelism(2);

        dataStream.print().setParallelism(1);

        env.execute("rocketmq-local-test");
    }
}
