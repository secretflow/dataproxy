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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.manager.SplitReader;

import java.io.IOException;
import java.util.List;

/**
 * Binary file data split reader
 *
 * @author muhong
 * @date 2023-09-13 21:47
 */
@Slf4j
public class BinaryFileSplitReader extends ArrowReader implements SplitReader {

    private static final String FIELD_NAME = "binary_data";
    private static final int BATCH_SIZE = 3 * 1024 * 1024;
    private final FSDataInputStream inputStream;

    public BinaryFileSplitReader(BufferAllocator allocator,
                                 FileSystem fileSystem,
                                 String uri) {
        super(allocator);

        // Generate file input stream
        try {
            this.inputStream = fileSystem.open(new Path(uri));
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_READ_STREAM_CREATE_FAILED, e);
        }
    }

    @Override
    public ArrowReader startRead() {
        return this;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();
        VarBinaryVector binaryVector = (VarBinaryVector) root.getVector(FIELD_NAME);
        binaryVector.allocateNew(1);

        // 申请足够空间
        while (binaryVector.getDataBuffer().capacity() < BATCH_SIZE) {
            binaryVector.reallocDataBuffer();
        }

        int length = downloadRangeToBuffer(binaryVector.getDataBuffer());
        if (length == 0) {
            return false;
        }

        binaryVector.getOffsetBuffer().setInt(VarBinaryVector.OFFSET_WIDTH, length);
        BitVectorHelper.setBit(binaryVector.getValidityBuffer(), 0);
        binaryVector.setLastSet(0);

        root.setRowCount(1);
        return true;
    }

    @Override
    public long bytesRead() {
        try {
            return this.inputStream.available();
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.GET_FILE_SIZE_FAILED, e);
        }
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            if (this.inputStream != null) {
                this.inputStream.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return new Schema(List.of(Field.notNullable(FIELD_NAME, new ArrowType.Binary())));
    }

    private int downloadRangeToBuffer(ArrowBuf valueBuffer) {
        if (inputStream == null) {
            return 0;
        }

        try {
            if (inputStream.available() == 0) {
                return 0;
            }

            byte[] bytes = new byte[BATCH_SIZE];
            int length = inputStream.read(bytes);
            valueBuffer.writeBytes(bytes, 0, length);
            return length;
        } catch (IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_BATCH_DOWNLOAD_FAILED, e);
        }
    }
}
