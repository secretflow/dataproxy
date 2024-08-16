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

package org.secretflow.dataproxy.manager.connector.filesystem;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.secretflow.dataproxy.common.model.dataset.format.CSVFormatConfig;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.SplitReader;

import java.util.List;

/**
 * CSV file data reader
 *
 * @author muhong
 * @date 2023-09-11 12:00
 */
public class CSVDataReader implements DataReader {

    private final FileSystem fileSystem;

    private final String uri;

    private final BufferAllocator allocator;

    private final CSVFormatConfig formatConfig;

    private final List<String> fieldList;

    private final Schema schema;

    public CSVDataReader(BufferAllocator allocator,
                         FileSystem fileSystem,
                         String uri,
                         Schema schema,
                         CSVFormatConfig formatConfig,
                         List<String> fieldList) {
        this.allocator = allocator;
        this.fileSystem = fileSystem;
        this.uri = uri;
        this.schema = schema;
        this.formatConfig = formatConfig;
        this.fieldList = fieldList;
    }

    @Override
    public List<SplitReader> createSplitReader(int splitNumber) {
        return List.of(new CSVSplitReader(allocator, fileSystem, uri, schema, formatConfig, fieldList));
    }
}
