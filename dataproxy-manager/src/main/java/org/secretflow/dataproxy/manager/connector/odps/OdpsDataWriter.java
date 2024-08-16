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


import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.DataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * odps Table Writer
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
@Slf4j
public class OdpsDataWriter implements DataWriter {


    private final OdpsConnConfig connConfig;

    private final OdpsTableInfo tableInfo;

    private final Schema schema;

    private final boolean overwrite = true;

    private TableTunnel.UploadSession uploadSession = null;
    private RecordWriter recordWriter = null;

    public OdpsDataWriter(OdpsConnConfig connConfig, OdpsTableInfo tableInfo, Schema schema) throws TunnelException, IOException {
        this.connConfig = connConfig;
        this.tableInfo = tableInfo;
        this.schema = schema;
        initOdps();
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {

        final int batchSize = root.getRowCount();
        log.info("odps writer batchSize: {}", batchSize);
        int columnCount = root.getFieldVectors().size();

        TableSchema tableSchema = uploadSession.getSchema();

        Record record;
        String columnName;

        for (int rowIndex = 0; rowIndex < batchSize; rowIndex++) {
            record = uploadSession.newRecord();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                log.debug("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex).getField().getType());
                columnName = root.getVector(columnIndex).getField().getName();

                if (tableSchema.containsColumn(columnName)) {
                    this.setRecordValue(record, tableSchema.getColumnIndex(columnName), this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
                } else {
                    log.warn("column: `{}` not exists in table: {}", columnName, tableInfo.tableName());
                }

            }
            recordWriter.write(record);
            log.debug("record: {}", record);
        }

    }

    @Override
    public void flush() throws IOException {
        try {
            if (recordWriter != null) {
                recordWriter.close();
            }
            if (uploadSession != null) {
                uploadSession.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy() throws IOException {

    }

    @Override
    public void close() throws Exception {
        // odps no close function
    }

    private Odps initOdpsClient(OdpsConnConfig odpsConnConfig) {

        if (odpsConnConfig == null) {
            throw new IllegalArgumentException("connConfig is null");
        }

        return OdpsUtil.buildOdps(odpsConnConfig);
    }

    private void initOdps() throws TunnelException, IOException {
        // init odps client
        Odps odps = initOdpsClient(this.connConfig);
        // Pre-processing
        preProcessing(odps, connConfig.getProjectName(), tableInfo.tableName());
        // init download session
        TableTunnel tunnel = new TableTunnel(odps);
        if (tableInfo.partitionSpec() != null && !tableInfo.partitionSpec().isEmpty()) {
            PartitionSpec partitionSpec = new PartitionSpec(tableInfo.partitionSpec());
            uploadSession = tunnel.createUploadSession(connConfig.getProjectName(), tableInfo.tableName(), partitionSpec, overwrite);
        } else {
            uploadSession = tunnel.createUploadSession(connConfig.getProjectName(), tableInfo.tableName(), overwrite);
        }

        recordWriter = uploadSession.openRecordWriter(0);
    }

    /**
     * 类型不匹配时需要做处理，未处理的报错
     * TODO: 双方类型绑定设计，未绑定时报错提示，当前先做简单类型转换
     * 例： Record: FLOAT、DOUBLE -> Arrow: floatingpoint
     *
     * @param record      ODPS记录
     * @param columnIndex 列索引
     * @param value       值
     */
    private void setRecordValue(Record record, int columnIndex, Object value) {
        if (value == null) {
            record.set(columnIndex, null);
            return;
        }

        Column column = record.getColumns()[columnIndex];

        OdpsType odpsType = column.getTypeInfo().getOdpsType();
        log.debug("record odps type: {}", odpsType);
        switch (odpsType) {
            case STRING -> record.setString(columnIndex, String.valueOf(value));
            case FLOAT -> record.set(columnIndex, Float.parseFloat(String.valueOf(value)));
            case DOUBLE -> record.set(columnIndex, Double.parseDouble(String.valueOf(value)));
            case BIGINT -> record.set(columnIndex, Long.parseLong(String.valueOf(value)));
            case INT -> record.set(columnIndex, Integer.parseInt(String.valueOf(value)));
            default -> record.set(columnIndex, value);
        }
    }

    /**
     * 获取字段数据
     *
     * @param fieldVector field vector
     * @param index       index
     * @return value
     */
    private Object getValue(FieldVector fieldVector, int index) {
        if (fieldVector == null || index < 0) {
            return null;
        }
        ArrowType.ArrowTypeID arrowTypeID = fieldVector.getField().getType().getTypeID();

        switch (arrowTypeID) {
            case Int -> {
                if (fieldVector instanceof IntVector || fieldVector instanceof BigIntVector || fieldVector instanceof SmallIntVector) {
                    return fieldVector.getObject(index);
                }
            }
            case FloatingPoint -> {
                if (fieldVector instanceof Float4Vector | fieldVector instanceof Float8Vector) {
                    return fieldVector.getObject(index);
                }
            }
            case Utf8 -> {
                if (fieldVector instanceof VarCharVector vector) {
                    return new String(vector.get(index), StandardCharsets.UTF_8);
                }
            }
            case Null -> {
                return null;
            }
            default -> {
                log.warn("Not implemented type: {}, will use default function", arrowTypeID);
                return fieldVector.getObject(index);
            }

        }
        return null;
    }

    /**
     * Pre-processing
     * <br>1. 表存在校验，不存在时创建表
     *
     * @param odps        odps client
     * @param projectName project name
     * @param tableName   table name
     */
    private void preProcessing(Odps odps, String projectName, String tableName) {

        if (!isExistsTable(odps, projectName, tableName)) {
            boolean odpsTable = createOdpsTable(odps, projectName, tableName, schema);
            if (!odpsTable) {
                throw DataproxyException.of(DataproxyErrorCode.ODPS_CREATE_TABLE_FAILED);
            }
        }
        log.info("odps table is exists or create table successful, project: {}, table name: {}", projectName, tableName);
    }

    /**
     * check Table is exist
     *
     * @param odps        odps client
     * @param projectName project name
     * @param tableName   table name
     * @return true or false
     */
    private boolean isExistsTable(Odps odps, String projectName, String tableName) {
        try {
            return odps.tables().exists(projectName, tableName);
        } catch (Exception e) {
            log.error("check exists table error, projectName:{}, tableName:{}", projectName, tableName, e);
        }
        return false;
    }

    private boolean createOdpsTable(Odps odps, String projectName, String tableName, Schema schema) {
        try {
            odps.tables().create(projectName, tableName, convertToTableSchema(schema), true);
            return true;
        } catch (Exception e) {
            log.error("create odps table error, projectName:{}, tableName:{}", projectName, tableName, e);
        }
        return false;
    }

    private TableSchema convertToTableSchema(Schema schema) {
        List<Column> columns = schema.getFields().stream().map(this::convertToColumn).toList();
        return TableSchema.builder().withColumns(columns).build();
    }

    private Column convertToColumn(Field field) {
        return Column.newBuilder(field.getName(), convertToType(field.getType())).build();
    }

    private TypeInfo convertToType(ArrowType type) {

        ArrowType.ArrowTypeID arrowTypeID = type.getTypeID();

        switch (arrowTypeID) {
            case Utf8 -> {
                return TypeInfoFactory.STRING;
            }
            case FloatingPoint -> {

                return switch (((ArrowType.FloatingPoint) type).getPrecision()) {
                    case SINGLE -> TypeInfoFactory.FLOAT;
                    case DOUBLE -> TypeInfoFactory.DOUBLE;
                    default -> TypeInfoFactory.UNKNOWN;
                };
            }
            case Int -> {
                return TypeInfoFactory.INT;
            }
            case Time -> {
                return TypeInfoFactory.TIMESTAMP;
            }
            case Date -> {
                return TypeInfoFactory.DATE;
            }
            default -> {
                log.warn("Not implemented type: {}", arrowTypeID);
                return TypeInfoFactory.UNKNOWN;
            }
        }
    }
}
