/*
 * Copyright 2024 Ant Group Co., Ltd.
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
package org.secretflow.dataproxy.manager.connector.odps;

import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.memory.BufferAllocator;
import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.format.DatasetFormatTypeEnum;
import org.secretflow.dataproxy.common.model.datasource.DatasourceConnConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasourceTypeEnum;
import org.secretflow.dataproxy.common.model.datasource.conn.ConnConfig;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.LocationConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.Connector;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.DataWriter;

import java.io.IOException;
import java.util.Objects;

/**
 * odps Connector
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
public class OdpsConnector implements Connector {

    /**
     * odps connection config
     */
    private final OdpsConnConfig config;

    public OdpsConnector(ConnConfig config) {
        if (!(config instanceof OdpsConnConfig odpsConnConfig)) {
            throw new IllegalArgumentException("Invalid conn config type.");
        }
        this.config = odpsConnConfig;
    }

    @Override
    public InferSchemaResult inferSchema(BufferAllocator allocator, LocationConfig locationConfig, DatasetFormatConfig formatConfig) {

        return InferSchemaResult.builder()
                .datasetFormatConfig(formatConfig)
                .schema(null)
                .build();

    }

    @Override
    public DataReader buildReader(BufferAllocator allocator, DatasetReadCommand readCommand) {

        if (invalidateConnectionType(readCommand.getConnConfig())) {
            throw new IllegalArgumentException("[ODPS] Unsupported datasource type.");
        }

        if (Objects.equals(DatasetFormatTypeEnum.TABLE, readCommand.getFormatConfig().getType())) {
            return new OdpsDataReader(allocator, config, (OdpsTableInfo) readCommand.getLocationConfig().getLocationConfig(), readCommand.getSchema());
        }
        return new OdpsResourceReader(allocator, config, (OdpsTableInfo) readCommand.getLocationConfig().getLocationConfig());
    }

    @Override
    public DataWriter buildWriter(DatasetWriteCommand writeCommand) {

        if (invalidateConnectionType(writeCommand.getConnConfig())) {
            throw new IllegalArgumentException("[ODPS] Unsupported datasource type.");
        }
        OdpsTableInfo locationConfig = (OdpsTableInfo) writeCommand.getLocationConfig().getLocationConfig();

        if (Objects.equals(DatasetFormatTypeEnum.TABLE, writeCommand.getFormatConfig().getType())) {
            try {
                return new OdpsDataWriter(config, locationConfig, writeCommand.getSchema());
            } catch (TunnelException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new OdpsResourceWriter(config, locationConfig);
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void close() throws Exception {
        // odps no function to close
    }

    /**
     * 判断是否为无效的 type
     *
     * @param connConfig 连接配置
     * @return boolean true 表示无效
     */
    private boolean invalidateConnectionType(DatasourceConnConfig connConfig) {
        if (connConfig == null || connConfig.getType() == null) {
            return true;
        }
        return connConfig.getType() != DatasourceTypeEnum.ODPS;
    }
}
