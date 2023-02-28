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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

public class RocketMQSinkTest {

    @Test
    public void buildTest() {
        RocketMQSink<String> rocketmqSink = RocketMQSink.<String>builder()
                .setEndpoints("localhost:9876")
                .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, "accessKey")
                .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, "secretKey")
                .setSerializer((RocketMQSerializationSchema<String>) (element, context, timestamp) ->
                        new Message("sink-topic", "tag", element.getBytes()))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
