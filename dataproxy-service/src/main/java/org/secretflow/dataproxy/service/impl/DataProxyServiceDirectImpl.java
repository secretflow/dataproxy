/*
 * Copyright 2023 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.commons.collections4.CollectionUtils;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasetLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasourceConnConfig;
import org.secretflow.dataproxy.common.model.datasource.conn.JdbcBaseConnConfig;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.manager.Connector;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.DataWriter;
import org.secretflow.dataproxy.manager.connector.filesystem.FileSystemConnector;
import org.secretflow.dataproxy.manager.connector.odps.OdpsConnector;
import org.secretflow.dataproxy.manager.connector.rdbms.JdbcConnector;
import org.secretflow.dataproxy.service.DataProxyService;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * 简单数据处理中心实现（数据直传）
 *
 * @author muhong
 * @date 2023-09-01 17:12
 */
@Slf4j
@Service
public class DataProxyServiceDirectImpl implements DataProxyService {

    protected Cache<String, Connector> connectorCache;

    @PostConstruct
    private void init() {
        connectorCache = Caffeine.newBuilder()
            .maximumSize(100)
            .removalListener((key, connector, cause) -> {
                if (connector != null) {
                    try {
                        ((Connector) connector).close();
                        log.info("[DataProxyServiceDirectImpl] remove item from connector cache success, cause:{}, key:{}", cause, key);
                    } catch (Exception e) {
                        log.error("[DataProxyServiceDirectImpl] remove item from connector cache failed, because connector close failed, conn config: {}",
                            key, e);
                    }
                }
            })
            .build();
    }

    /**
     * 构建数据源连接器
     *
     * @param connConfig 数据源连接信息
     * @return 数据源连接器
     */
    @Override
    public synchronized Connector buildConnector(DatasourceConnConfig connConfig) {
        String key = connConfig.generateUniqueId();

        Connector connector = connectorCache.getIfPresent(key);
        if (connector != null) {
            if (connector.isAvailable()) {
                return connector;
            } else {
                connectorCache.invalidate(key);
            }
        }

        if (connConfig.getType() == null) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_NOT_EXIST_ERROR, "数据源类型字段缺失");
        }

        switch (connConfig.getType()) {
            case MYSQL: {
                // 连接信息缺失校验
                if (connConfig.getConnConfig() == null) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_NOT_EXIST_ERROR, "数据源连接信息字段缺失");
                }
                connector = new JdbcConnector(connConfig.getType(), (JdbcBaseConnConfig) connConfig.getConnConfig());
                break;
            }
            case MINIO:
            case OSS:
            case OBS:
            case LOCAL_HOST:
                connector = new FileSystemConnector(connConfig.getType(), connConfig.getConnConfig());
                break;
            case ODPS:
                connector = new OdpsConnector(connConfig.getConnConfig());
                break;
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的数据源类型 " + connConfig.getType());
        }
        connectorCache.put(key, connector);
        return connector;
    }

    @Override
    public void validateConn(DatasourceConnConfig connConfig) {
        // 能构建出connector，就说明连接正常
        buildConnector(connConfig);
    }

    @Override
    public InferSchemaResult inferSchema(BufferAllocator allocator, DatasourceConnConfig connConfig, DatasetLocationConfig locationConfig, DatasetFormatConfig formatConfig) {
        Connector connector = buildConnector(connConfig);
        return connector.inferSchema(allocator, locationConfig.getLocationConfig(), formatConfig);
    }

    @Override
    public ArrowReader generateArrowReader(BufferAllocator allocator, DatasetReadCommand readCommand) {
        Connector connector = buildConnector(readCommand.getConnConfig());

        // 补充formatConfig中缺失参数
        InferSchemaResult inferSchemaResult = inferSchema(allocator, readCommand.getConnConfig(),
            readCommand.getLocationConfig(), readCommand.getFormatConfig());
        readCommand.setFormatConfig(inferSchemaResult.getDatasetFormatConfig());
        // 当schema缺省时进行推断
        if (readCommand.getSchema() == null) {
            readCommand.setSchema(inferSchemaResult.getSchema());
        }

        DataReader dataReader = connector.buildReader(allocator, readCommand);
        return dataReader.createSplitReader(1).get(0).startRead();
    }

    @Override
    public void datasetWrite(DatasetWriteCommand writeCommand, FlightStream flightStream, WriteCallback writeCallback) {

        try (Connector connector = buildConnector(writeCommand.getConnConfig())) {
            VectorSchemaRoot batch = flightStream.getRoot();

            if (writeCommand.getSchema() == null || CollectionUtils.isEmpty(writeCommand.getSchema().getFields())) {
                writeCommand.setSchema(batch.getSchema());
            }

            try (DataWriter dataWriter = connector.buildWriter(writeCommand)) {
                while (flightStream.next()) {
                    dataWriter.write(batch);
                    // 调用写回调
                    writeCallback.ack(batch);
                    log.info("[datasetWrite] 数据块存储成功");
                }
                dataWriter.flush();
                log.info("[datasetWrite] dataset write over");
            }
        } catch (DataproxyException e) {
            log.error("[datasetWrite] dataset write error, cmd: {}", JsonUtils.toJSONString(writeCommand), e);
            throw e;
        } catch (Exception e) {
            log.error("[datasetWrite] dataset write unknown exception, cmd: {}", JsonUtils.toJSONString(writeCommand), e);
            throw DataproxyException.of(DataproxyErrorCode.DATASET_WRITE_ERROR, e);
        }
    }

}