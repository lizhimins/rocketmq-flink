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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.rocketmq.legacy.common.util.RetryUtil;
import org.apache.flink.connector.rocketmq.source.config.SourceConfiguration;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.MessageQueueOffsets;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.util.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class InnerConsumerImpl implements InnerConsumer {

    public InnerConsumerImpl(SourceConfiguration sourceConfiguration) {
        String accessKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            AclClientRPCHook aclClientRPCHook =
                    new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
    }

    @Override
    public CompletableFuture<Long> seekCommittedOffset(MessageQueue messageQueue, String consumerGroup) {
        return null;
    }

    @Override
    public CompletableFuture<Long> seekMinOffset(MessageQueue messageQueue) {
        return null;
    }

    @Override
    public CompletableFuture<Long> seekMaxOffset(MessageQueue messageQueue) {
        return null;
    }

    @Override
    public CompletableFuture<Long> seekOffsetsForTimestamp(MessageQueue messageQueue, long timestamp) {
        return null;
    }

    @Override
    public CompletableFuture<List<MessageView>> pullBlockIfNotFound(MessageQueue messageQueue, long offset) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitOffset(MessageQueue messageQueue, long offset) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * The implementation for offsets retriever with a consumer and an admin client.
     */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements MessageQueueOffsets.MessageQueueOffsetsRetriever, AutoCloseable {

        private final DefaultLitePullConsumer consumer;
        private final DefaultMQAdminExt adminExt;

        public PartitionOffsetsRetrieverImpl(DefaultLitePullConsumer consumer, DefaultMQAdminExt adminExt) {
            this.consumer = consumer;
            this.adminExt = adminExt;
        }

        @Override
        public void close() throws Exception {
            this.adminExt.shutdown();
        }

        @Override
        public Map<MessageQueue, Long> committedOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();

            //Method fetchMethod = null;
            //if (offsetStore instanceof RemoteBrokerOffsetStore) {
            //    try {
            //        fetchMethod = RemoteBrokerOffsetStore.class.getMethod("fetchConsumeOffsetFromBroker");
            //    } catch (NoSuchMethodException e) {
            //        throw new RuntimeException(e);
            //    }
            //}

            //Method finalFetchMethod = fetchMethod;

            OffsetStore offsetStore = consumer.getOffsetStore();
            for (MessageQueue messageQueue : messageQueues) {
                long offset = RetryUtil.call(
                        () -> offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE),
                        "fetch offset from broker failed");
                offsetMap.put(messageQueue, offset);
            }
            return offsetMap;
        }

        @Override
        public Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
            for (MessageQueue messageQueue : messageQueues) {
                try {
                    long offset = this.adminExt.minOffset(messageQueue);
                    offsetMap.put(messageQueue, offset);
                } catch (MQClientException e) {
                    throw new RuntimeException(e);
                }
            }
            return offsetMap;
        }

        @Override
        public Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
            for (MessageQueue messageQueue : messageQueues) {
                try {
                    long offset = this.adminExt.maxOffset(messageQueue);
                    offsetMap.put(messageQueue, offset);
                } catch (MQClientException e) {
                    throw new RuntimeException(e);
                }
            }
            return offsetMap;
        }

        @Override
        public Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
            List<CompletableFuture<Long>> futureList = new ArrayList<>(messageQueueWithTimeMap.size());
            for (Map.Entry<MessageQueue, Long> entry : messageQueueWithTimeMap.entrySet()) {
                CompletableFuture<Long> future = CompletableFuture.completedFuture(
                                RetryUtil.call(() ->
                                                adminExt.searchOffset(entry.getKey(), entry.getValue()),
                                        "failed to search offset by timestamp"))
                        .thenApply(aLong -> {
                            offsetMap.put(entry.getKey(), aLong);
                            return null;
                        });
                futureList.add(future);
            }
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
            return offsetMap;
        }
    }
}
