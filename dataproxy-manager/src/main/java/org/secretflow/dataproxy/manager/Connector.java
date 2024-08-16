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

package org.secretflow.dataproxy.manager;

import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.location.LocationConfig;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Datasource connector
 *
 * @author muhong
 * @date 2023-09-01 18:04
 */
public interface Connector extends AutoCloseable {

    /**
     * infer schema
     *
     * @param allocator      Arrow data allocator
     * @param locationConfig Dataset location
     * @param formatConfig   Dataset format
     * @return Infer result
     */
    InferSchemaResult inferSchema(BufferAllocator allocator, LocationConfig locationConfig, DatasetFormatConfig formatConfig);

    /**
     * Build dataset reader
     *
     * @param allocator   Arrow data allocator
     * @param readCommand Read command
     * @return Reader
     */
    DataReader buildReader(BufferAllocator allocator, DatasetReadCommand readCommand);

    /**
     * Build dataset writer
     *
     * @param writeCommand Write command
     * @return Writer
     */
    DataWriter buildWriter(DatasetWriteCommand writeCommand);

    /**
     * Check connector status
     * @return
     */
    boolean isAvailable();
}