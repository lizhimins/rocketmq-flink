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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** A serializer used to serialize {@link RocketMQCommittable}. */
public class RocketMQCommittableSerializer
        implements SimpleVersionedSerializer<RocketMQCommittable> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQCommittable obj) throws IOException {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(stream)) {
            // TxnID txnID = obj.getTxnID();
            // out.writeLong(txnID.getMostSigBits());
            // out.writeLong(txnID.getLeastSigBits());
            out.writeUTF(obj.getTopic());
            out.flush();
            return stream.toByteArray();
        }
    }

    @Override
    public RocketMQCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            // long mostSigBits = in.readLong();
            // long leastSigBits = in.readLong();
            String topic = in.readUTF();
            return new RocketMQCommittable(topic);
        }
    }
}
