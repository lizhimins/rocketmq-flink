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

package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.connector.rocketmq.source.split.RocketMQPartitionSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the enumerator state of RocketMQ source.
 */
public class RocketMQSourceEnumStateSerializer
        implements SimpleVersionedSerializer<RocketMQSourceEnumState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQSourceEnumState enumState) throws IOException {
        Set<RocketMQPartitionSplit> assignedPartitions = enumState.getAssignedPartitions();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            out.writeInt(assignedPartitions.size());
            for (RocketMQPartitionSplit split : assignedPartitions) {
                out.writeUTF(split.getTopicName());
                out.writeUTF(split.getBrokerName());
                out.writeInt(split.getPartitionId());
                out.writeLong(split.getStartingOffset());
                out.writeLong(split.getStoppingOffset());
            }
            out.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public RocketMQSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // Check whether the version of serialized bytes is supported.
        if (version == getVersion()) {
            return new RocketMQSourceEnumState(deserializeTopicPartitions(serialized));
        }
        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, getVersion()));
    }

    private static Set<RocketMQPartitionSplit> deserializeTopicPartitions(byte[] serializedTopicPartitions)
            throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedTopicPartitions);
             DataInputStream in = new DataInputStream(byteArrayInputStream)) {

            final int numPartitions = in.readInt();
            Set<RocketMQPartitionSplit> topicPartitions = new HashSet<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                final String topicName = in.readUTF();
                final String brokerName = in.readUTF();
                final int partitionId = in.readInt();
                final long startingOffset = in.readLong();
                final long stoppingOffset = in.readLong();
                topicPartitions.add(new RocketMQPartitionSplit(
                        topicName, brokerName, partitionId, startingOffset, stoppingOffset));
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return topicPartitions;
        }
    }
}
