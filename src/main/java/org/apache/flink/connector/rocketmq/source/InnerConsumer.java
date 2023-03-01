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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface InnerConsumer extends AutoCloseable {

    CompletableFuture<Map<String /*topic*/, TopicRouteData>> getTopicRoute(List<String> topicList);

    CompletableFuture<Long /*offset*/> seekCommittedOffset(MessageQueue messageQueue, String consumerGroup);

    CompletableFuture<Long /*offset*/> seekMinOffset(MessageQueue messageQueue);

    CompletableFuture<Long /*offset*/> seekMaxOffset(MessageQueue messageQueue);

    CompletableFuture<Long /*offset*/> seekOffsetsForTimestamp(MessageQueue messageQueue, long timestamp);

    CompletableFuture<List<MessageView>> pullBlockIfNotFound(MessageQueue messageQueue, long offset);

    CompletableFuture<Void> commitOffset(MessageQueue messageQueue, long offset);
}
