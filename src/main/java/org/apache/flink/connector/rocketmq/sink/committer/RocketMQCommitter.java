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

package org.apache.flink.connector.rocketmq.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.connector.rocketmq.sink.config.SinkConfiguration;
import org.apache.flink.connector.rocketmq.source.config.SourceConfiguration;

import java.util.Collection;

/**
 * Committer implementation for {@link RocketMQSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
public class RocketMQCommitter implements Committer<RocketMQCommittable>, Cloneable {

    public RocketMQCommitter(SinkConfiguration sinkConfiguration) {

    }

    @Override
    public void commit(Collection<CommitRequest<RocketMQCommittable>> requests) {

    }

    @Override
    public void close() throws Exception {

    }
}
