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
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.MessageViewExt;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InnerConsumerImpl implements InnerConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InnerConsumerImpl.class);

    private final SourceConfiguration sourceConfiguration;
    private final DefaultMQAdminExt adminExt;
    private final DefaultLitePullConsumer consumer;

    public InnerConsumerImpl(SourceConfiguration sourceConfiguration) {
        this.sourceConfiguration = sourceConfiguration;
        String accessKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = sourceConfiguration.getString(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);

        // Note: sync pull thread num may not enough
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            AclClientRPCHook aclClientRpcHook =
                    new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
            this.adminExt = new DefaultMQAdminExt(aclClientRpcHook);
            this.consumer = new DefaultLitePullConsumer(aclClientRpcHook);
        } else {
            this.adminExt = new DefaultMQAdminExt();
            this.consumer = new DefaultLitePullConsumer();
        }

        this.consumer.setNamesrvAddr(sourceConfiguration.getEndpoints());
        this.consumer.setConsumerGroup(sourceConfiguration.getConsumerGroup());
        this.consumer.setAutoCommit(false);
        this.consumer.setVipChannelEnabled(false);
        this.consumer.setInstanceName(String.join("#", ManagementFactory.getRuntimeMXBean().getName(),
                sourceConfiguration.getConsumerGroup(), UUID.randomUUID().toString()));

        this.adminExt.setNamesrvAddr(sourceConfiguration.getEndpoints());
        this.adminExt.setAdminExtGroup(sourceConfiguration.getConsumerGroup());
        this.adminExt.setVipChannelEnabled(false);
    }

    @Override
    public void start() throws MQClientException {
        this.adminExt.start();
        this.consumer.start();
        LOG.info("Consumer started success, group={}, consumerId={}",
                this.consumer.getConsumerGroup(), this.consumer.getInstanceName());
    }

    @Override
    public void close() throws Exception {
        this.adminExt.shutdown();
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
        return this.consumer.poll(timeout.toMillis()).stream()
                .map((Function<MessageExt, MessageView>) MessageViewExt::new).collect(Collectors.toList());
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        this.consumer.seek(messageQueue, offset);
        LOG.info("Consumer set message queue offset, mq={}, offset={}",
                UtilAll.getDescribeString(messageQueue), offset);
    }

    @Override
    public void pause(Collection<MessageQueue> messageQueues) {
        this.consumer.pause(messageQueues);
        LOG.info("Consumer pause message queue, mq={}", messageQueues);
    }

    @Override
    public void resume(Collection<MessageQueue> messageQueues) {
        this.consumer.resume(messageQueues);
        LOG.info("Consumer resume message queue, mq={}", messageQueues);
    }

    @Override
    public CompletableFuture<Long> seekCommittedOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(() -> {
            long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
            LOG.error("Consumer seek committed offset, mq={}, offset={}",
                    UtilAll.getDescribeString(messageQueue), offset);
            return offset;
        });
    }

    @Override
    public CompletableFuture<Long> seekMinOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long offset = adminExt.minOffset(messageQueue);
                LOG.info("Consumer seek min offset, mq={}, offset={}",
                        UtilAll.getDescribeString(messageQueue), offset);
                return offset;
            } catch (MQClientException e) {
                LOG.error("Consumer seek min offset error", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> seekMaxOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long offset = adminExt.maxOffset(messageQueue);
                LOG.info("Consumer seek max offset, mq={}, offset={}",
                        UtilAll.getDescribeString(messageQueue), offset);
                return offset;
            } catch (MQClientException e) {
                LOG.error("Consumer seek max offset error, mq={}",
                        UtilAll.getDescribeString(messageQueue), e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> seekOffsetForTimestamp(MessageQueue messageQueue, long timestamp) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long offset = adminExt.searchOffset(messageQueue, timestamp);
                LOG.info("Consumer seek offset for timestamp, mq={}, timestamp={}, offset={}",
                        UtilAll.getDescribeString(messageQueue), timestamp, offset);
                return offset;
            } catch (MQClientException e) {
                LOG.error("Consumer seek offset for timestamp error, mq={}, timestamp={}",
                        UtilAll.getDescribeString(messageQueue), timestamp, e);
                throw new RuntimeException(e);
            }
        });
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
            implements OffsetsSelector.MessageQueueOffsetsRetriever, AutoCloseable {

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
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(messageQueues.stream().map(messageQueue ->
                    CompletableFuture.supplyAsync(() -> innerConsumer.seekCommittedOffset(messageQueue))
                            .thenAccept(future -> {
                                try {
                                    result.put(messageQueue, future.get());
                                } catch (Exception e) {
                                    LOG.error("Consumer fetch committed offset error", e);
                                }
                            })).toArray(CompletableFuture[]::new)).join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(messageQueues.stream().map(messageQueue ->
                    CompletableFuture.supplyAsync(() -> innerConsumer.seekMinOffset(messageQueue))
                            .thenAccept(future -> {
                                try {
                                    result.put(messageQueue, future.get());
                                } catch (Exception e) {
                                    LOG.error("Consumer fetch min offset error", e);
                                }
                            })).toArray(CompletableFuture[]::new)).join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(messageQueues.stream().map(messageQueue ->
                    CompletableFuture.supplyAsync(() -> innerConsumer.seekMaxOffset(messageQueue))
                            .thenAccept(future -> {
                                try {
                                    result.put(messageQueue, future.get());
                                } catch (Exception e) {
                                    LOG.error("Consumer fetch committed offset error", e);
                                }
                            })).toArray(CompletableFuture[]::new)).join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> offsetsForTimes(Map<MessageQueue, Long> messageQueueWithTimeMap) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(messageQueueWithTimeMap.entrySet().stream().map(entry ->
                    CompletableFuture.supplyAsync(() ->
                                    innerConsumer.seekOffsetForTimestamp(entry.getKey(), entry.getValue()))
                            .thenAccept(future -> {
                                try {
                                    result.put(entry.getKey(), future.get());
                                } catch (Exception e) {
                                    LOG.error("Consumer fetch offset by timestamp error", e);
                                }
                            })).toArray(CompletableFuture[]::new)).join();
            return result;
        }
    }
}
