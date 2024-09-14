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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.SplitReader;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * odps Table Split Reader
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
@Slf4j
public class OdpsSplitArrowReader extends ArrowReader implements SplitReader, AutoCloseable {

    private final OdpsConnConfig odpsConnConfig;

    private final OdpsTableInfo tableInfo;

    private final Schema schema;

    private TableSchema tableSchema;

    private final int batchSize = 1000;

    private ResultSet resultSet;

    private final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

    protected OdpsSplitArrowReader(BufferAllocator allocator, OdpsConnConfig odpsConnConfig, OdpsTableInfo tableInfo, Schema schema) {
        super(allocator);
        this.odpsConnConfig = odpsConnConfig;
        this.tableInfo = tableInfo;
        this.schema = schema;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();

        ValueVectorUtility.preAllocate(root, batchSize);
        Record next;

        int recordCount = 0;
        if (!resultSet.hasNext()) {
            return false;
        }
        while (resultSet.hasNext()) {
            next = resultSet.next();
            if (next != null) {

                ValueVectorUtility.ensureCapacity(root, recordCount + 1);
                toArrowVector(next, root, recordCount);
                recordCount++;
            }

            if (recordCount == batchSize) {
                root.setRowCount(recordCount);
                return true;
            }
        }
        root.setRowCount(recordCount);
        return true;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {

    }

    @Override
    protected Schema readSchema() throws IOException {
        return this.schema;
    }

    @Override
    public ArrowReader startRead() {

        Odps odps = OdpsUtil.buildOdps(odpsConnConfig);
        String sql = "";
        try {
            sql = this.buildSql(tableInfo.tableName(), tableInfo.fields(), tableInfo.partitionSpec());
            log.debug("SQLTask run sql: {}", sql);

            Instance instance = SQLTask.run(odps, sql);
            // 等待任务完成
            instance.waitForSuccess();

            resultSet = SQLTask.getResultSet(instance);

            tableSchema = resultSet.getTableSchema();

        } catch (OdpsException e) {
            log.error("SQLTask run error, sql: {}", sql, e);
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, e.getMessage(), e);
        } catch (IOException e) {
            log.error("startRead error, sql: {}", sql, e);
            throw new RuntimeException(e);
        }

        return this;
    }

    private String buildSql(String tableName, List<String> fields, String whereClause) {

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        return "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause) + ";";
    }

    private void toArrowVector(Record record, VectorSchemaRoot root, int rowIndex) {
        FieldVector vector;
        String columnName;
        for (Field field : schema.getFields()) {
            vector = root.getVector(field);
            if (vector != null) {
                columnName = field.getName();
                if (tableSchema.containsColumn(columnName)) {
                    setValue(vector.getField().getType(), vector, rowIndex, record, columnName);
                    vector.setValueCount(rowIndex + 1);
                }
            }
        }
    }

    private void setValue(ArrowType type, FieldVector vector, int rowIndex, Record record, String columnName) {
        log.debug("columnName: {} type ID: {}, value: {}", columnName, type.getTypeID(), record.get(columnName));
        if (record.get(columnName) == null) {
            return;
        }
        switch (type.getTypeID()) {
            case Int -> {
                if (vector instanceof IntVector intVector) {
                    intVector.setSafe(rowIndex, Integer.parseInt(record.get(columnName).toString()));
                } else if (vector instanceof BigIntVector bigIntVector) {
                    bigIntVector.setSafe(rowIndex, Long.parseLong(record.get(columnName).toString()));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }
            case Utf8 -> {
                if (vector instanceof VarCharVector varcharVector) {
                    // record#getBytes default is UTF-8
                    varcharVector.setSafe(rowIndex, record.getBytes(columnName));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }
            case FloatingPoint -> {
                if (vector instanceof Float4Vector floatVector) {
                    floatVector.setSafe(rowIndex, Float.parseFloat(record.get(columnName).toString()));
                } else if (vector instanceof Float8Vector doubleVector) {
                    doubleVector.setSafe(rowIndex, Double.parseDouble(record.get(columnName).toString()));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }

            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        }

    }
}
