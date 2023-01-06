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

package org.apache.flink.connector.rocketmq.source.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.connector.rocketmq.source.RocketMQOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSourceFactory} implementation to create {@link
 * RocketMQScanTableSource}.
 */
public class RocketMQDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public String factoryIdentifier() {
        return "rocketmq";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(RocketMQOptions.TOPIC);
        requiredOptions.add(RocketMQOptions.CONSUMER_GROUP);
        requiredOptions.add(RocketMQOptions.NAME_SERVER_ADDRESS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(RocketMQOptions.OPTIONAL_TAG);
        optionalOptions.add(RocketMQOptions.OPTIONAL_SQL);
        optionalOptions.add(RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET);
        optionalOptions.add(RocketMQOptions.OPTIONAL_START_TIME_MILLS);
        optionalOptions.add(RocketMQOptions.OPTIONAL_START_TIME);
        optionalOptions.add(RocketMQOptions.OPTIONAL_END_TIME);
        optionalOptions.add(RocketMQOptions.OPTIONAL_TIME_ZONE);
        optionalOptions.add(RocketMQOptions.OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        optionalOptions.add(RocketMQOptions.OPTIONAL_USE_NEW_API);
        optionalOptions.add(RocketMQOptions.OPTIONAL_ENCODING);
        optionalOptions.add(RocketMQOptions.OPTIONAL_FIELD_DELIMITER);
        optionalOptions.add(RocketMQOptions.OPTIONAL_LINE_DELIMITER);
        optionalOptions.add(RocketMQOptions.OPTIONAL_COLUMN_ERROR_DEBUG);
        optionalOptions.add(RocketMQOptions.OPTIONAL_LENGTH_CHECK);
        optionalOptions.add(RocketMQOptions.OPTIONAL_ACCESS_KEY);
        optionalOptions.add(RocketMQOptions.OPTIONAL_SECRET_KEY);
        optionalOptions.add(RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE);
        optionalOptions.add(RocketMQOptions.OPTIONAL_CONSUMER_POLL_MS);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String topic = configuration.getString(RocketMQOptions.TOPIC);
        String consumerGroup = configuration.getString(RocketMQOptions.CONSUMER_GROUP);
        String nameServerAddress = configuration.getString(RocketMQOptions.NAME_SERVER_ADDRESS);
        String tag = configuration.getString(RocketMQOptions.OPTIONAL_TAG);
        String sql = configuration.getString(RocketMQOptions.OPTIONAL_SQL);
        if (configuration.contains(RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE)
                && (configuration.contains(RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET)
                        || configuration.contains(RocketMQOptions.OPTIONAL_START_TIME_MILLS)
                        || configuration.contains(RocketMQOptions.OPTIONAL_START_TIME))) {
            throw new IllegalArgumentException(
                    String.format(
                            "cannot support these configs when %s has been set: [%s, %s, %s] !",
                            RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE.key(),
                            RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET.key(),
                            RocketMQOptions.OPTIONAL_START_TIME.key(),
                            RocketMQOptions.OPTIONAL_START_TIME_MILLS.key()));
        }
        long startMessageOffset =
                configuration.getLong(RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET);
        long startTimeMs = configuration.getLong(RocketMQOptions.OPTIONAL_START_TIME_MILLS);
        String startDateTime = configuration.getString(RocketMQOptions.OPTIONAL_START_TIME);
        String timeZone = configuration.getString(RocketMQOptions.OPTIONAL_TIME_ZONE);
        String accessKey = configuration.getString(RocketMQOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(RocketMQOptions.OPTIONAL_SECRET_KEY);
        long startTime = startTimeMs;
        if (startTime == -1) {
            if (!StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
                try {
                    startTime = parseDateString(startDateTime, timeZone);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Incorrect datetime format: %s, pls use ISO-8601 "
                                            + "complete date plus hours, minutes and seconds format:%s.",
                                    startDateTime, DATE_FORMAT),
                            e);
                }
            }
        }
        long stopInMs = Long.MAX_VALUE;
        String endDateTime = configuration.getString(RocketMQOptions.OPTIONAL_END_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(endDateTime)) {
            try {
                stopInMs = parseDateString(endDateTime, timeZone);
            } catch (ParseException e) {
                throw new RuntimeException(
                        String.format(
                                "Incorrect datetime format: %s, pls use ISO-8601 "
                                        + "complete date plus hours, minutes and seconds format:%s.",
                                endDateTime, DATE_FORMAT),
                        e);
            }
            Preconditions.checkArgument(
                    stopInMs >= startTime, "Start time should be less than stop time.");
        }
        long partitionDiscoveryIntervalMs =
                configuration.getLong(RocketMQOptions.OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        boolean useNewApi = configuration.getBoolean(RocketMQOptions.OPTIONAL_USE_NEW_API);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        descriptorProperties.putTableSchema("schema", physicalSchema);
        String consumerOffsetMode =
                configuration.getString(
                        RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE,
                        RocketMQConfig.CONSUMER_OFFSET_LATEST);
        long consumerOffsetTimestamp =
                configuration.getLong(
                        RocketMQOptions.OPTIONAL_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis());
        return new RocketMQScanTableSource(
                configuration.getLong(RocketMQOptions.OPTIONAL_CONSUMER_POLL_MS),
                descriptorProperties,
                physicalSchema,
                topic,
                consumerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                sql,
                stopInMs,
                startMessageOffset,
                startMessageOffset < 0 ? startTime : -1L,
                partitionDiscoveryIntervalMs,
                consumerOffsetMode,
                consumerOffsetTimestamp,
                useNewApi);
    }

    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat =
                FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone(timeZone));
        return simpleDateFormat.parse(dateString).getTime();
    }
}
