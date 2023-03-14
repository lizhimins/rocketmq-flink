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

package org.apache.flink.connector.rocketmq.example;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.KEY_PREFIX;
import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.MESSAGE_NUM;
import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.SOURCE_TOPIC_1;
import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.TAGS;
import static org.apache.flink.connector.rocketmq.example.ConnectorExampleNew.getAclRpcHook;

public class SimpleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {

        DefaultMQProducer producer = new DefaultMQProducer("PID-FLINK",
                getAclRpcHook(), true, null);
        producer.setNamesrvAddr(ENDPOINTS);
        producer.setAccessChannel(AccessChannel.CLOUD);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < MESSAGE_NUM; i++) {
            String content = "Test Message " + i;
            Message msg = new Message(SOURCE_TOPIC_1, TAGS, KEY_PREFIX + i, content.getBytes());
            try {
                SendResult sendResult = producer.send(msg);
                assert sendResult != null;
                System.out.printf(
                        "send result: %s %s\n",
                        sendResult.getMsgId(), sendResult.getMessageQueue().toString());
            } catch (Exception e) {
                LOGGER.info("send message failed. {}", e.toString());
            }
        }
    }
}
