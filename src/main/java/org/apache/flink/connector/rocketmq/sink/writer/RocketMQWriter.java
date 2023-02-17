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

package org.apache.flink.connector.rocketmq.sink.writer;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.sink.committer.RocketMQCommittable;
import org.apache.flink.connector.rocketmq.sink.producer.FlinkRocketMQInternalProducer;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class RocketMQWriter<IN>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String KAFKA_PRODUCER_METRICS = "producer-metrics";

    private final DeliveryGuarantee deliveryGuarantee;

    private final RocketMQSerializationSchema<IN> serializationSchema;

    //private final Callback deliveryCallback;
    private final RocketMQSinkContext rocketmqSinkContext;

    //private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    //private final SinkWriterMetricGroup metricGroup;
    //private final boolean disabledMetrics;
    //private final Counter numRecordsOutCounter;
    //private final Counter numBytesOutCounter;
    //private final Counter numRecordsOutErrorsCounter;
    //private final ProcessingTimeService timeService;

    // Number of outgoing bytes at the latest metric sync
    //private long latestOutgoingByteTotal;
    //private Metric byteOutMetric;
    private FlinkRocketMQInternalProducer currentProducer;

    //private final RocketMQWriterState kafkaWriterState;

    // producer pool only used for exactly once
    //private final Deque<FlinkKafkaInternalProducer<byte[], byte[]>> producerPool =
    //        new ArrayDeque<>();
    private final Closer closer = Closer.create();
    private long lastCheckpointId;

    private boolean closed = false;
    private long lastSync = System.currentTimeMillis();

    public RocketMQWriter(
            DeliveryGuarantee deliveryGuarantee,
            RocketMQSerializationSchema<IN> serializationSchema,
            RocketMQSinkContext rocketmqSinkContext,
            FlinkRocketMQInternalProducer currentProducer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.serializationSchema = serializationSchema;
        this.rocketmqSinkContext = rocketmqSinkContext;
        this.currentProducer = currentProducer;
        this.lastCheckpointId = lastCheckpointId;
    }

    @Override
    public Collection<RocketMQCommittable> prepareCommit() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {

    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {

    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        TwoPhaseCommittingSink.PrecommittingSinkWriter.super.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {

    }
}
