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

package org.secretflow.dataproxy.plugin.odps.writer;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.dataproxy.plugin.odps.config.OdpsCommandConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConfigConstant;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsWriteConfig;
import org.secretflow.dataproxy.plugin.odps.utils.OdpsUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author yuexie
 * @date 2024/11/10 21:50
 **/
@Slf4j
public class OdpsRecordWriter implements Writer {

    private final OdpsCommandConfig<?> commandConfig;
    private final OdpsConnectConfig odpsConnectConfig;
    private final OdpsTableConfig odpsTableConfig;

    private TableTunnel.UploadSession uploadSession = null;
    private TableSchema odpsTableSchema = null;
    private RecordWriter recordWriter = null;
    private boolean isPartitioned = false;

    public OdpsRecordWriter(OdpsWriteConfig commandConfig) {
        this.commandConfig = commandConfig;
        this.odpsConnectConfig = commandConfig.getOdpsConnectConfig();
        this.odpsTableConfig = commandConfig.getCommandConfig();
        this.prepare();
    }

    @Override
    public void write(VectorSchemaRoot root) {
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
                // odps column name is lower case
                columnName = root.getVector(columnIndex).getField().getName().toLowerCase();

                if (tableSchema.containsColumn(columnName)) {
                    this.setRecordValue(record, tableSchema.getColumnIndex(columnName), this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
                } else {
                    log.warn("column: `{}` not exists in table: {}", columnName, odpsTableConfig.tableName());
                }

            }
            try {
                recordWriter.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.debug("record: {}", record);
        }
    }

    @Override
    public void flush() {
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

    private Odps initOdpsClient(OdpsConnectConfig odpsConnectConfig) {
        if (odpsConnectConfig == null) {
            throw new IllegalArgumentException("connConfig is null");
        }
        return OdpsUtil.initOdps(odpsConnectConfig);
    }

    private void prepare() {
        try {
            // init odps client
            Odps odps = initOdpsClient(odpsConnectConfig);
            // Pre-processing
            PartitionSpec convertPartitionSpec = this.convertToPartitionSpec(odpsTableConfig.partition());
            preProcessing(odps, odpsConnectConfig.projectName(), odpsTableConfig.tableName(), convertPartitionSpec);
            // init upload session
            TableTunnel tunnel = new TableTunnel(odps);

            if (isPartitioned) {
                if (odpsTableConfig.partition() == null || odpsTableConfig.partition().isEmpty()) {
                    throw DataproxyException.of(DataproxyErrorCode.INVALID_PARTITION_SPEC, "partitionSpec is empty");
                }
                assert this.odpsTableSchema != null;
                List<Column> partitionColumns = this.odpsTableSchema.getPartitionColumns();
                PartitionSpec partitionSpec = new PartitionSpec();
                for (Column partitionColumn : partitionColumns) {
                    partitionSpec.set(partitionColumn.getName(), convertPartitionSpec.get(partitionColumn.getName()));
                }
                uploadSession = tunnel.createUploadSession(odpsConnectConfig.projectName(), odpsTableConfig.tableName(), partitionSpec, OdpsUtil.OVER_WRITE);
            } else {
                uploadSession = tunnel.createUploadSession(odpsConnectConfig.projectName(), odpsTableConfig.tableName(), OdpsUtil.OVER_WRITE);
            }

            recordWriter = uploadSession.openRecordWriter(0, true);
        } catch (OdpsException | IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "An exception occurred while writer initializing", e);
        }
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
            log.warn("table name: {} record set null value. index: {}", odpsTableConfig.tableName(), columnIndex);
            return;
        }

        Column column = record.getColumns()[columnIndex];

        OdpsType odpsType = column.getTypeInfo().getOdpsType();
        log.debug("record odps type: {}", odpsType);
        switch (odpsType) {
            case STRING -> record.setString(columnIndex, String.valueOf(value));
            case FLOAT -> record.set(columnIndex, Float.parseFloat(String.valueOf(value)));
            case DOUBLE -> record.set(columnIndex, Double.parseDouble(String.valueOf(value)));
            case TINYINT -> record.set(columnIndex, Byte.parseByte(String.valueOf(value)));
            case SMALLINT -> record.set(columnIndex, Short.parseShort(String.valueOf(value)));
            case BIGINT -> record.set(columnIndex, Long.parseLong(String.valueOf(value)));
            case INT -> record.set(columnIndex, Integer.parseInt(String.valueOf(value)));
            case BOOLEAN -> record.setBoolean(columnIndex, (Boolean) value);
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
        if (fieldVector == null || index < 0 || fieldVector.getObject(index) == null) {
            return null;
        }
        ArrowType.ArrowTypeID arrowTypeID = fieldVector.getField().getType().getTypeID();

        switch (arrowTypeID) {
            case Int -> {
                if (fieldVector instanceof IntVector || fieldVector instanceof BigIntVector || fieldVector instanceof SmallIntVector || fieldVector instanceof TinyIntVector) {
                    return fieldVector.getObject(index);
                }
                log.warn("Type INT is not IntVector or BigIntVector or SmallIntVector or TinyIntVector, value is: {}", fieldVector.getObject(index).toString());
            }
            case FloatingPoint -> {
                if (fieldVector instanceof Float4Vector | fieldVector instanceof Float8Vector) {
                    return fieldVector.getObject(index);
                }
                log.warn("Type FloatingPoint is not Float4Vector or Float8Vector, value is: {}", fieldVector.getObject(index).toString());
            }
            case Utf8 -> {
                if (fieldVector instanceof VarCharVector vector) {
                    return new String(vector.get(index), StandardCharsets.UTF_8);
                }
                log.warn("Type Utf8 is not VarCharVector, value is: {}", fieldVector.getObject(index).toString());
            }
            case Null -> {
                return null;
            }
            case Bool -> {
                if (fieldVector instanceof BitVector vector) {
                    return vector.get(index) == 1;
                }
                log.warn("Type BOOL is not BitVector, value is: {}", fieldVector.getObject(index).toString());
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
    private void preProcessing(Odps odps, String projectName, String tableName, PartitionSpec partitionSpec) throws OdpsException {

        if (!isExistsTable(odps, projectName, tableName)) {
            boolean odpsTable = createOdpsTable(odps, projectName, tableName, commandConfig.getResultSchema(), partitionSpec);
            if (!odpsTable) {
                throw DataproxyException.of(DataproxyErrorCode.ODPS_CREATE_TABLE_FAILED);
            }
            log.info("odps table is not exists, create table successful, project: {}, table name: {}", projectName, tableName);
        } else {
            log.info("odps table is exists, project: {}, table name: {}", projectName, tableName);
        }

        Table table = odps.tables().get(projectName, tableName);
        isPartitioned = table.isPartitioned();
        this.setOdpsTableSchemaIfAbsent(table.getSchema());

        if (isPartitioned) {
            if (partitionSpec == null || partitionSpec.isEmpty()) {
                throw DataproxyException.of(DataproxyErrorCode.INVALID_PARTITION_SPEC, "partitionSpec is empty");
            }

            if (!isExistsPartition(odps, projectName, tableName, partitionSpec)) {
                boolean odpsPartition = createOdpsPartition(odps, projectName, tableName, partitionSpec);
                if (!odpsPartition) {
                    throw DataproxyException.of(DataproxyErrorCode.ODPS_CREATE_PARTITION_FAILED);
                }
                log.info("odps partition is not exists, create partition successful, project: {}, table name: {}, PartitionSpec: {}", projectName, tableName, partitionSpec);
            } else {
                log.info("odps partition is exists, project: {}, table name: {}, PartitionSpec: {}", projectName, tableName, partitionSpec);
            }

        }
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

    private boolean isExistsPartition(Odps odps, String projectName, String tableName, PartitionSpec partitionSpec) throws OdpsException {
        Table table = odps.tables().get(projectName, tableName);

        if (table == null) {
            log.warn("table is null, projectName:{}, tableName:{}", projectName, tableName);
            throw DataproxyException.of(DataproxyErrorCode.ODPS_TABLE_NOT_EXISTS);
        }

        return table.hasPartition(partitionSpec);
    }

    /**
     * create odps table
     *
     * @param odps          odps client
     * @param projectName   project name
     * @param tableName     table name
     * @param schema        schema
     * @param partitionSpec partition spec
     * @return true or false
     */
    private boolean createOdpsTable(Odps odps, String projectName, String tableName, Schema schema, PartitionSpec partitionSpec) {
        try {
            TableSchema tableSchema = convertToTableSchema(schema);
            if (partitionSpec != null) {
                // Infer partitioning field type as string.
                List<Column> tableSchemaColumns = tableSchema.getColumns();
                List<Integer> partitionColumnIndexes = new ArrayList<>();
                ArrayList<Column> partitionColumns = new ArrayList<>();

                for (String key : partitionSpec.keys()) {
                    if (tableSchema.containsColumn(key)) {
                        log.info("tableSchemaColumns contains partition column: {}", key);
                        partitionColumnIndexes.add(tableSchema.getColumnIndex(key));
                        partitionColumns.add(tableSchema.getColumn(key));
                    } else {
                        log.info("tableSchemaColumns not contains partition column: {}", key);
                        partitionColumns.add(Column.newBuilder(key, TypeInfoFactory.STRING).build());
                    }
                }

                for (int i = 0; i < partitionColumnIndexes.size(); i++) {
                    tableSchemaColumns.remove(partitionColumnIndexes.get(i) - i);
                }
                log.info("tableSchemaColumns: {}, partitionColumnIndexes: {}", JsonUtils.toString(tableSchemaColumns), JsonUtils.toString(partitionColumnIndexes));
                tableSchema.setColumns(tableSchemaColumns);
                tableSchema.setPartitionColumns(partitionColumns);
            }
            log.info("create odps table schema: {}", JsonUtils.toString(tableSchema));
            Optional<Long> tableLifeCycleFromConfig = this.getTableLifeCycleValueConfig();
            odps.tables().create(projectName, tableName, tableSchema, "", true, tableLifeCycleFromConfig.orElse(null), OdpsUtil.getSqlFlag(), null);
            return true;
        } catch (Exception e) {
            log.error("create odps table error, projectName:{}, tableName:{}", projectName, tableName, e);
        }
        return false;
    }

    private boolean createOdpsPartition(Odps odps, String projectName, String tableName, PartitionSpec partitionSpec) {
        try {
            Table table = odps.tables().get(projectName, tableName);
            table.createPartition(partitionSpec, true);
            return true;
        } catch (Exception e) {
            log.error("create odps partition error, projectName:{}, tableName:{}", projectName, tableName, e);
        }
        return false;
    }

    private void setOdpsTableSchemaIfAbsent(TableSchema tableSchema) {
        if (odpsTableSchema == null) {
            this.odpsTableSchema = tableSchema;
        }
    }

    private TableSchema convertToTableSchema(Schema schema) {
        List<Column> columns = schema.getFields().stream().map(this::convertToColumn).toList();
        return TableSchema.builder().withColumns(columns).build();
    }

    /**
     * convert partition spec
     *
     * @param partitionSpec partition spec
     * @return partition spec
     * @throws IllegalArgumentException if partitionSpec is invalid
     */
    private PartitionSpec convertToPartitionSpec(String partitionSpec) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return null;
        }
        return new PartitionSpec(partitionSpec);
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
                return switch (((ArrowType.Int) type).getBitWidth()) {
                    case 8 -> TypeInfoFactory.TINYINT;
                    case 16 -> TypeInfoFactory.SMALLINT;
                    case 32 -> TypeInfoFactory.INT;
                    case 64 -> TypeInfoFactory.BIGINT;
                    default -> TypeInfoFactory.UNKNOWN;
                };
            }
            case Time -> {
                return TypeInfoFactory.TIMESTAMP;
            }
            case Date -> {
                return TypeInfoFactory.DATE;
            }
            case Bool -> {
                return TypeInfoFactory.BOOLEAN;
            }
            default -> {
                log.warn("Not implemented type: {}", arrowTypeID);
                return TypeInfoFactory.UNKNOWN;
            }
        }
    }

    /**
     * Get the one that needs to be set to the ODPS table lifecycle from the configuration. <br>
     * If the configuration is set, return an Optional of the value. <br>
     * If the configuration is not set, return an empty Optional. <br>
     *
     * @return optional of the value of the configuration
     */
    public Optional<Long> getTableLifeCycleValueConfig() {
        return Optional.ofNullable(FlightServerContext.get(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE, Long.class));
    }

}
