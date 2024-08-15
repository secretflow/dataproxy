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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
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
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.SplitReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * odps Resource Split Reader
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
@Slf4j
public class OdpsResourceSplitReader extends ArrowReader implements SplitReader {

    private static final String FIELD_NAME = "binary_data";
    private static final int BATCH_SIZE = 3 * 1024 * 1024;

    private final OdpsConnConfig odpsConnConfig;

    private final OdpsTableInfo tableInfo;

    private InputStream inputStream;


    private int readIndex = 0;

    protected OdpsResourceSplitReader(BufferAllocator allocator, OdpsConnConfig odpsConnConfig, OdpsTableInfo tableInfo) {
        super(allocator);
        this.odpsConnConfig = odpsConnConfig;
        this.tableInfo = tableInfo;
    }

    @Override
    public ArrowReader startRead() {

        Odps odps = OdpsUtil.buildOdps(odpsConnConfig);
        try {
            inputStream = odps.resources().getResourceAsStream(tableInfo.tableName());
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();

        VarBinaryVector vector = (VarBinaryVector) root.getVector(FIELD_NAME);
        vector.allocateNew(1);

        // 申请足够空间
        while (vector.getDataBuffer().capacity() < BATCH_SIZE) {
            vector.reallocDataBuffer();
        }

        ArrowBuf dataBuffer = vector.getDataBuffer();

        int l = readRangeToBuffer(dataBuffer, 0);
        if (l == 0) {
            return false;
        }

        readIndex += l;

        vector.getOffsetBuffer().setInt(VarBinaryVector.OFFSET_WIDTH, l);
        BitVectorHelper.setBit(vector.getValidityBuffer(), 0);
        vector.setLastSet(0);

        root.setRowCount(1);

        return true;
    }

    @Override
    public long bytesRead() {

        try {
            if (inputStream != null) {
                return inputStream.available();
            }
            throw DataproxyException.of(DataproxyErrorCode.FILE_READ_STREAM_CREATE_FAILED);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            if (Objects.nonNull(inputStream)) {
                inputStream.close();
            }
        } catch (IOException ignored) {

        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return new Schema(List.of(Field.notNullable(FIELD_NAME, new ArrowType.Binary())));
    }

    private int readRangeToBuffer(ArrowBuf valueBuffer, int startIndex) {
        if (inputStream == null) {
            return 0;
        }

        try {
            if (inputStream.available() == 0) {
                return 0;
            }

            byte[] bytes = new byte[1024];
            int length = inputStream.read(bytes);
            valueBuffer.writeBytes(bytes, startIndex, length);
            return length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
