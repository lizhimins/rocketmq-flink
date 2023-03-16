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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplitState;

import java.util.Map;
import java.util.function.Supplier;

/** The source reader for RocketMQ partitions. */
public class RocketMQSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
        MessageView, T, RocketMQSourceSplit, RocketMQSourceSplitState> {

    private final RocketMQSourceReaderMetrics rocketMQSourceReaderMetrics;

    public RocketMQSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageView>> elementsQueue,
            Supplier<SplitReader<MessageView, RocketMQSourceSplit>> splitReaderSupplier,
            RecordEmitter<MessageView, T, RocketMQSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            RocketMQSourceReaderMetrics rocketMQSourceReaderMetrics) {

        super(elementsQueue, splitReaderSupplier, recordEmitter, config, context);
        this.rocketMQSourceReaderMetrics = rocketMQSourceReaderMetrics;
    }

    @Override
    protected void onSplitFinished(Map<String, RocketMQSourceSplitState> map) {}

    @Override
    protected RocketMQSourceSplitState initializedState(RocketMQSourceSplit partitionSplit) {
        return new RocketMQSourceSplitState(partitionSplit);
    }

    @Override
    protected RocketMQSourceSplit toSplitType(
            String splitId, RocketMQSourceSplitState splitState) {
        return splitState.toRocketMQPartitionSplit();
    }
}
