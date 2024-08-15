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


import com.aliyun.odps.FileResource;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.DataWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * odps Resource Writer
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
@Slf4j
public class OdpsResourceWriter implements DataWriter {

    private final OdpsConnConfig odpsConnConfig;
    private final OdpsTableInfo odpsTableInfo;

    private Odps odps;

    private static final String FIELD_NAME = "binary_data";

    private InputStream odpsInputStream = null;

    public OdpsResourceWriter(OdpsConnConfig odpsConnConfig, OdpsTableInfo odpsTableInfo) {
        this.odpsConnConfig = odpsConnConfig;
        this.odpsTableInfo = odpsTableInfo;
        initOdps();
    }


    @Override
    public void write(VectorSchemaRoot root) throws IOException {

        FieldVector vector = root.getVector(FIELD_NAME);

        if (vector instanceof VarBinaryVector varBinaryVector) {

            int rowCount = root.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                byte[] bytes = varBinaryVector.get(row);

                odpsInputStream = new ByteArrayInputStream(bytes);
                FileResource fileResource = new FileResource();
                fileResource.setName(odpsTableInfo.tableName());
                try {
                    if (resourceExists(odps, odpsTableInfo.tableName())) {
                        odps.resources().update(fileResource, odpsInputStream);
                    } else {
                        odps.resources().create(fileResource, odpsInputStream);
                    }
                } catch (OdpsException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            throw DataproxyException.of(DataproxyErrorCode.UNSUPPORTED_FIELD_TYPE, "Only support VarBinaryVector type");
        }

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void destroy() throws IOException {

    }

    @Override
    public void close() throws Exception {
        if (odpsInputStream != null) {
            odpsInputStream.close();
        }
    }

    private void initOdps() {
        odps = OdpsUtil.buildOdps(odpsConnConfig);
    }

    private static boolean resourceExists(Odps odps, String resourceName) throws OdpsException {
        try {
            Resource resource = odps.resources().get(resourceName);
            resource.reload();
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        }
    }
}
