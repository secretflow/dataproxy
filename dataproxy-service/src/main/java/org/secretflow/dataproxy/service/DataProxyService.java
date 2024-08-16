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

package org.secretflow.dataproxy.service;

import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasetLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasourceConnConfig;
import org.secretflow.dataproxy.manager.Connector;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * @author muhong
 * @date 2023-09-01 11:01
 */
public interface DataProxyService {

    /**
     * 构造数据源连接器
     *
     * @param connConfig 数据源连接信息
     * @return 数据源连接器
     */
    Connector buildConnector(DatasourceConnConfig connConfig);

    /**
     * 校验数据源连接参数
     *
     * @param connConfig 数据源连接信息
     */

    void validateConn(DatasourceConnConfig connConfig);

    /**
     * 推断数据结构
     *
     * @param allocator      内存分配器
     * @param connConfig     数据源连接信息
     * @param locationConfig 数据集位置信息
     * @param formatConfig   数据集格式信息
     * @return 数据结构及详细格式信息
     */
    InferSchemaResult inferSchema(BufferAllocator allocator, DatasourceConnConfig connConfig, DatasetLocationConfig locationConfig, DatasetFormatConfig formatConfig);

    /**
     * 数据读取
     *
     * @param allocator   内存分配器
     * @param readCommand 数据读取指令
     * @return Arrow流式数据读取对象
     */
    ArrowReader generateArrowReader(BufferAllocator allocator, DatasetReadCommand readCommand);

    /**
     * 数据存储
     *
     * @param writeCommand  数据存储指令
     * @param flightStream  待存储Arrow数据流
     * @param writeCallback 单块存储完成回调
     */
    void datasetWrite(DatasetWriteCommand writeCommand, FlightStream flightStream, WriteCallback writeCallback);

    /**
     * 单块数据存储完成回调
     */
    interface WriteCallback {
        void ack(VectorSchemaRoot root);
    }
}