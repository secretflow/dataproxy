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

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.manager.DataWriter;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Binary file data writer
 *
 * @author muhong
 * @date 2023-09-13 22:14
 */
@Slf4j
public class BinaryFileDataWriter implements DataWriter {

    private final String FIELD_NAME = "binary_data";

    private FSDataOutputStream outputStream;

    public BinaryFileDataWriter(FileSystem fileSystem, String uri) {
        // 获取文件写入流
        try {
            fileSystem.delete(new Path(uri), true);
            this.outputStream = fileSystem.create(new Path(uri));
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_WRITE_STREAM_CREATE_FAILED, e.getMessage(), e);
        }
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        log.info("[BinaryFileDataWriter-write] received schema:{}", root.getSchema().toJson());
        VarBinaryVector binaryVector = (VarBinaryVector) root.getVector(FIELD_NAME);
        if (binaryVector == null) {
            throw DataproxyException.of(DataproxyErrorCode.BINARY_DATA_FIELD_NOT_EXIST);
        }
        log.info("[BinaryFileDataWriter-write] root row count:{}, vector value count: {}", root.getRowCount(), binaryVector.getValueCount());
        for (int row = 0; row < root.getRowCount(); row++) {
            byte[] item = binaryVector.get(row);
            if (item == null) {
                log.info("[BinaryFileDataWriter-write] row:{}, item is null, continue", row);
                continue;
            }
            log.info("[BinaryFileDataWriter-write] row:{}, length:{}, value:\n{}\n", row, item.length, new String(item));
            this.outputStream.write(item);
        }
    }

    @Override
    public void flush() throws IOException {
        this.outputStream.flush();
    }

    @Override
    public void destroy() throws IOException {

    }

    @Override
    public void close() throws Exception {
        if (this.outputStream != null) {
            this.outputStream.close();
        }
    }
}
