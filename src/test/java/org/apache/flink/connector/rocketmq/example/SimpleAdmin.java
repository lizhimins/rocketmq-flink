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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class SimpleAdmin implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SimpleAdmin.class);

    private final DefaultMQAdminExt adminExt;

    public SimpleAdmin() throws MQClientException {
        this.adminExt = new DefaultMQAdminExt(ConnectorExampleNew.getAclRpcHook(), 6 * 1000L);
        this.adminExt.setNamesrvAddr(ConnectorExampleNew.ENDPOINTS);
        this.adminExt.setVipChannelEnabled(false);
        this.adminExt.setAdminExtGroup("rocketmq-tools");
        this.adminExt.setInstanceName(adminExt.getAdminExtGroup().concat("-").concat(UUID.randomUUID().toString()));
        this.adminExt.start();
        log.info("initialize rocketmq simple admin tools success {}", adminExt.getInstanceName());
    }

    @Override
    public void close() {
        this.adminExt.shutdown();
    }

    private Set<String> getBrokerAddress() throws RemotingException, InterruptedException, MQClientException {
        return adminExt.examineTopicRouteInfo(ConnectorExampleNew.CLUSTER_NAME).getBrokerDatas()
                .stream().map(BrokerData::selectBrokerAddr).collect(Collectors.toSet());
    }

    private void recreateTestTopic(Set<String> brokerAddressSet)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {

        for (String brokerAddress : brokerAddressSet) {
            adminExt.createAndUpdateTopicConfig(brokerAddress,
                    new TopicConfig(ConnectorExampleNew.SOURCE_TOPIC_1,
                            4, 2, PermName.PERM_READ | PermName.PERM_WRITE));

            adminExt.createAndUpdateTopicConfig(brokerAddress,
                    new TopicConfig(ConnectorExampleNew.SOURCE_TOPIC_1,
                            2, 2, PermName.PERM_READ | PermName.PERM_WRITE));

            log.info("recreate topic success, brokerAddress={}", brokerAddress);
        }
    }

    private void recreateTestGroup(Set<String> brokerAddressSet)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {

        for (String brokerAddress : brokerAddressSet) {
            adminExt.deleteSubscriptionGroup(brokerAddress, ConnectorExampleNew.CONSUMER_GROUP, true);
            log.info("delete consumer group success, brokerAddress={}", brokerAddress);
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleAdmin simpleAdmin = new SimpleAdmin();
        Set<String> brokerAddressSet = simpleAdmin.getBrokerAddress();
        simpleAdmin.recreateTestGroup(brokerAddressSet);
        simpleAdmin.recreateTestTopic(brokerAddressSet);
        simpleAdmin.close();
    }
}
