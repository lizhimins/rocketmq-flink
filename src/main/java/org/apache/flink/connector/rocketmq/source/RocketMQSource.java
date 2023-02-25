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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.source.config.SourceConfiguration;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumState;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumStateSerializer;
import org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumerator;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQPartitionSplitReader;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQRecordEmitter;
import org.apache.flink.connector.rocketmq.source.reader.RocketMQSourceReader;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/** The Source implementation of RocketMQ. */
public class RocketMQSource<OUT>
        implements Source<OUT, RocketMQPartitionSplit, RocketMQSourceEnumState>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = -1L;
    private static final Logger log = LoggerFactory.getLogger(RocketMQSource.class);

    // Users can choose only one of the following ways to specify the topics to consume from.
    private final DefaultLitePullConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private final MessageQueueOffsets startingMessageQueueOffsets;
    private final MessageQueueOffsets stoppingMessageQueueOffsets;

    // The configurations.
    private final SourceConfiguration sourceConfiguration;

    // Boundedness
    private final Boundedness boundedness;

    // RocketMQ DeserializationSchema
    private final RocketMQDeserializationSchema<OUT> deserializationSchema;

    public RocketMQSource(
            DefaultLitePullConsumer consumer,
            MessageQueueOffsets startingMessageQueueOffsets,
            MessageQueueOffsets stoppingMessageQueueOffsets,
            Boundedness boundedness,
            RocketMQDeserializationSchema<OUT> deserializationSchema,
            SourceConfiguration sourceConfiguration) {
        this.consumer = consumer;
        this.startingMessageQueueOffsets = startingMessageQueueOffsets;
        this.stoppingMessageQueueOffsets = stoppingMessageQueueOffsets;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
    }

    /**
     * Get a RocketMQSourceBuilder to build a {@link RocketMQSourceBuilder}.
     *
     * @return a RocketMQ source builder.
     */
    public static <OUT> RocketMQSourceBuilder<OUT> builder() {
        return new RocketMQSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, RocketMQPartitionSplit> createReader(SourceReaderContext readerContext)
            throws Exception {

        FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageView>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });

        final RocketMQSourceReaderMetrics rocketMQSourceReaderMetrics =
                new RocketMQSourceReaderMetrics(readerContext.metricGroup());

        Supplier<SplitReader<MessageView, RocketMQPartitionSplit>> splitReaderSupplier =
                () ->
                        new RocketMQPartitionSplitReader<>(
                                sourceConfiguration,
                                readerContext,
                                deserializationSchema,
                                rocketMQSourceReaderMetrics);

        RocketMQRecordEmitter<OUT> recordEmitter = new RocketMQRecordEmitter<>();

        return new RocketMQSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                sourceConfiguration,
                readerContext,
                rocketMQSourceReaderMetrics);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext) {

        return new RocketMQSourceEnumerator(
                consumer,
                startingMessageQueueOffsets,
                stoppingMessageQueueOffsets,
                sourceConfiguration,
                enumContext);
    }

    @Override
    public SplitEnumerator<RocketMQPartitionSplit, RocketMQSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RocketMQPartitionSplit> enumContext,
            RocketMQSourceEnumState checkpoint) {

         return new RocketMQSourceEnumerator(consumer,
                 startingMessageQueueOffsets,
                 stoppingMessageQueueOffsets,
                 sourceConfiguration,
                 enumContext,
                 checkpoint.getAssignedPartitions());
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
