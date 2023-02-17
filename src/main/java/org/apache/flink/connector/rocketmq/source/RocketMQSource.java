/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumState;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumStateSerializer;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumerator;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQPartitionSplitReader;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQRecordEmitter;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQSourceReader;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.function.Supplier;

/** The Source implementation of RocketMQ. */
public class RocketMQSource<OUT>
        implements Source<OUT, RocketMQPartitionSplit, RocketMQSourceEnumState>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = -1L;
    private static final Logger log = LoggerFactory.getLogger(RocketMQSource.class);

    // Users can specify the starting / stopping offset initializer.
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;

    protected DefaultLitePullConsumer consumer;

    private String consumerOffsetMode;
    private long consumerOffsetTimestamp;
    private long pollTime;
    private String topic;
    private String consumerGroup;
    private String nameServerAddress;
    private String tag;
    private String sql;

    private String accessKey;
    private String secretKey;

    private long stopInMs;
    private long startTime;
    private long startOffset;
    private long partitionDiscoveryIntervalMs;

    // The configurations.
    protected Properties props;

    // Boundedness
    private Boundedness boundedness;
    private RocketMQDeserializationSchema<OUT> deserializationSchema;

    public RocketMQSource(
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            RocketMQDeserializationSchema<OUT> deserializationSchema,
            Properties props) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
        this.props = props;
    }

    /**
     * Get a RocketMQSourceBuilder to build a {@link RocketMQSourceBuilder}.
     *
     * @return a RocketMQ source builder.
     */
    public static <OUT> RocketMQSourceBuilder<OUT> builder() {
        return new RocketMQSourceBuilder<>();
    }

     //public RocketMQSource(
     //       long pollTime,
     //       String topic,
     //       String consumerGroup,
     //       String nameServerAddress,
     //       String accessKey,
     //       String secretKey,
     //       String tag,
     //       String sql,
     //       long stopInMs,
     //       long startTime,
     //       long startOffset,
     //       long partitionDiscoveryIntervalMs,
     //       Boundedness boundedness,
     //       RocketMQDeserializationSchema<OUT> deserializationSchema,
     //       String cosumerOffsetMode,
     //       long consumerOffsetTimestamp) {
     //   Validate.isTrue(
     //           !(StringUtils.isNotEmpty(tag) && StringUtils.isNotEmpty(sql)),
     //           "Consumer tag and sql can not set value at the same time");
     //   this.pollTime = pollTime;
     //   this.topic = topic;
     //   this.consumerGroup = consumerGroup;
     //   this.nameServerAddress = nameServerAddress;
     //   this.accessKey = accessKey;
     //   this.secretKey = secretKey;
     //   this.tag = StringUtils.isEmpty(tag) ? RocketMQConfig.DEFAULT_CONSUMER_TAG : tag;
     //   this.sql = sql;
     //   this.stopInMs = stopInMs;
     //   this.startTime = startTime;
     //   this.startOffset = startOffset > 0 ? startOffset : startTime;
     //   this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
     //   this.boundedness = boundedness;
     //   this.deserializationSchema = deserializationSchema;
     //   this.consumerOffsetMode = cosumerOffsetMode;
     //   this.consumerOffsetTimestamp = consumerOffsetTimestamp;
     //}

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, RocketMQPartitionSplit> createReader(
            SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<OUT, Long, Long>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        //deserializationSchema.open(
        //        new DeserializationSchema.InitializationContext() {
        //            @Override
        //            public MetricGroup getMetricGroup() {
        //                return readerContext.metricGroup();
        //            }
        //
        //            @Override
        //            public UserCodeClassLoader getUserCodeClassLoader() {
        //                return null;
        //            }
        //        });

        Supplier<SplitReader<Tuple3<OUT, Long, Long>, RocketMQPartitionSplit>> splitReaderSupplier =
                () ->
                        new RocketMQPartitionSplitReader<>(
                                pollTime,
                                topic,
                                consumerGroup,
                                nameServerAddress,
                                accessKey,
                                secretKey,
                                tag,
                                sql,
                                stopInMs,
                                startTime,
                                startOffset,
                                deserializationSchema);
        RocketMQRecordEmitter<OUT> recordEmitter = new RocketMQRecordEmitter<>();

        return new RocketMQSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                new Configuration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext) {

        return new RocketMQSourceEnumerator(
                topic,
                consumerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                stopInMs,
                startOffset,
                partitionDiscoveryIntervalMs,
                boundedness,
                enumContext,
                consumerOffsetMode,
                consumerOffsetTimestamp);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext,
            RocketMQSourceEnumState checkpoint) {

        return new RocketMQSourceEnumerator(
                topic,
                consumerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                stopInMs,
                startOffset,
                partitionDiscoveryIntervalMs,
                boundedness,
                enumContext,
                checkpoint.getCurrentAssignment(),
                consumerOffsetMode,
                consumerOffsetTimestamp);
    }

    @Override
    public SimpleVersionedSerializer<RocketMQPartitionSplit> getSplitSerializer() {
        return new RocketMQPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<RocketMQSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new RocketMQSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
