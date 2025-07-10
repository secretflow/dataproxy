/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.plugin.hive.producer;

import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.producer.AbstractDatabaseFlightProducer;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseDoGetContext;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;
import org.secretflow.dataproxy.plugin.hive.utils.HiveUtil;

public class HiveFlightProducer extends AbstractDatabaseFlightProducer {

    @Override
    public String getProducerName() {
        return "hive";
    }

    @Override
    protected DatabaseDoGetContext initDoGetContext(DatabaseCommandConfig<?> config) {
        return new DatabaseDoGetContext(config, HiveUtil::initHive, HiveUtil::buildQuerySql, HiveUtil::jdbcType2ArrowType);
    }

    @Override
    protected DatabaseRecordWriter initRecordWriter(DatabaseWriteConfig config) {
        return new DatabaseRecordWriter(config,
                HiveUtil::initHive,
                HiveUtil::buildCreateTableSql,
                HiveUtil::buildMultiRowInsertSql,
                HiveUtil::checkTableExists);
    }
}

