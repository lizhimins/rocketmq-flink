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
import org.apache.flink.connector.rocketmq.source.config.SourceConfiguration;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsStrategy;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.util.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.common.message.MessageQueue;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class InnerConsumerImpl implements InnerConsumer {

    private SourceConfiguration sourceConfiguration;

    private DefaultLitePullConsumerImpl pullConsumer;

    private final DefaultLitePullConsumer consumer;

    public InnerConsumerImpl(SourceConfiguration sourceConfiguration) {

        this.sourceConfiguration = sourceConfiguration;

        String accessKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            AclClientRPCHook aclClientRpcHook =
                    new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
            this.consumer = new DefaultLitePullConsumer(aclClientRpcHook);
        } else {
            this.consumer = new DefaultLitePullConsumer();
        }

        this.consumer.setNamesrvAddr(sourceConfiguration.getEndpoints());
        this.consumer.setConsumerGroup(sourceConfiguration.getConsumerGroup());
        this.consumer.setVipChannelEnabled(false);
        this.consumer.setInstanceName(String.join("||", ManagementFactory.getRuntimeMXBean().getName(),
                sourceConfiguration.getConsumerGroup(), Long.toString(System.nanoTime())));
    }

    @Override
    public void start() throws MQClientException {
        this.consumer.start();
    }

    @Override
    public void close() throws Exception {
        this.consumer.shutdown();
    }

    @Override
    public String getConsumerGroup() {
        return this.sourceConfiguration.getConsumerGroup();
    }

    @Override
    public CompletableFuture<Collection<MessageQueue>> fetchMessageQueues(String topic) throws MQClientException {
        return CompletableFuture.completedFuture(consumer.fetchMessageQueues(topic));
    }

    @Override
    public void assign(Collection<MessageQueue> messageQueues) {
        this.consumer.assign(messageQueues);
    }

    @Override
    public List<MessageView> poll(Duration timeout) {
        return null;
    }

    @Override
    public void wakeup() {

    }

    @Override
    public void seek(MessageQueue messageQueue, long offset) {

    }

    @Override
    public void pause(Collection<MessageQueue> messageQueues) {

    }

    @Override
    public void resume(Collection<MessageQueue> messageQueues) {

    }

    @Override
    public CompletableFuture<Long> seekCommittedOffset(MessageQueue messageQueue) {
        //Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
        //OffsetStore offsetStore = consumer.getOffsetStore();
        //for (MessageQueue messageQueue : messageQueues) {
        //    long offset = RetryUtil.call(
        //            () -> offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE),
        //            "fetch offset from broker failed");
        //    offsetMap.put(messageQueue, offset);
        //}
        //return offsetMap;
        return null;
    }

    @Override
    public CompletableFuture<Long> seekMinOffset(MessageQueue messageQueue) {
        //Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
        //for (MessageQueue messageQueue : messageQueues) {
        //    try {
        //        long offset = this.adminExt.minOffset(messageQueue);
        //        offsetMap.put(messageQueue, offset);
        //    } catch (MQClientException e) {
        //        throw new RuntimeException(e);
        //    }
        //}
        //return offsetMap;
        return null;
    }

    @Override
    public CompletableFuture<Long> seekMaxOffset(MessageQueue messageQueue) {
        //Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();
        //for (MessageQueue messageQueue : messageQueues) {
        //    try {
        //        long offset = this.adminExt.maxOffset(messageQueue);
        //        offsetMap.put(messageQueue, offset);
        //    } catch (MQClientException e) {
        //        throw new RuntimeException(e);
        //    }
        //}
        //return offsetMap;
        return null;
    }

    @Override
    public CompletableFuture<Long> seekOffsetForTimestamp(MessageQueue messageQueue, long timestamp) {
        //List<CompletableFuture<Long>> futureList = new ArrayList<>(messageQueueWithTimeMap.size());
        //for (Map.Entry<MessageQueue, Long> entry : messageQueueWithTimeMap.entrySet()) {
        //    CompletableFuture<Long> future = CompletableFuture.completedFuture(
        //                    RetryUtil.call(() ->
        //                                    adminExt.searchOffset(entry.getKey(), entry.getValue()),
        //                            "failed to search offset by timestamp"))
        //            .thenApply(aLong -> {
        //                offsetMap.put(entry.getKey(), aLong);
        //                return null;
        //            });
        //    futureList.add(future);
        //}
        //CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        return null;
    }

    @Override
    public CompletableFuture<Void> commitOffset(MessageQueue messageQueue, long offset) {
        return null;
    }

    /**
     * The implementation for offsets retriever with a consumer and an admin client.
     */
    @VisibleForTesting
    public static class RemotingOffsetsRetrieverImpl
            implements OffsetsStrategy.MessageQueueOffsetsRetriever, AutoCloseable {

        private final InnerConsumer innerConsumer;

        public RemotingOffsetsRetrieverImpl(InnerConsumer innerConsumer) {
            this.innerConsumer = innerConsumer;
        }

        @Override
        public void close() throws Exception {
            this.innerConsumer.close();
        }

        @Override
        public Map<MessageQueue, Long> committedOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();

            return offsetMap;
        }

        @Override
        public Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues) {
            return new ConcurrentHashMap<>();
        }

        @Override
        public Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues) {
            return new ConcurrentHashMap<>();
        }

        @Override
        public Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap) {
            Map<MessageQueue, Long> offsetMap = new ConcurrentHashMap<>();

            return offsetMap;
        }
    }
}
