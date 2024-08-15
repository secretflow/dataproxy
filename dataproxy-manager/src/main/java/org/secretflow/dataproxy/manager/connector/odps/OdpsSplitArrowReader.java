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
import com.aliyun.odps.utils.StringUtils;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
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

    private String buildSql(String tableName, List<String> fields, String partition) {

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        String transformedPartition = buildWhereClause(partition);
        return "select " + String.join(",", fields) + " from " + tableName + (transformedPartition.isEmpty() ? "" : " where " + transformedPartition) + ";";
    }

    /**
     * 过时方法，后续删除
     *
     * @param partition 分区字段
     * @return boolean
     */
    @Deprecated
    private String transformPartition(String partition) {

        Map<String, List<String>> fieldValuesMap = new HashMap<>();

        if (partition != null) {
            String[] split = StringUtils.split(partition, ';');
            for (String s : split) {
                String[] kv = StringUtils.split(s, '=');
                if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
                    throw DataproxyException.of(DataproxyErrorCode.INVALID_PARTITION_SPEC);
                }
                if (fieldValuesMap.containsKey(kv[0])) {
                    fieldValuesMap.get(kv[0]).add(kv[1]);
                } else {
                    fieldValuesMap.put(kv[0], new ArrayList<>(List.of(kv[1])));
                }
            }
        }

        return buildEqualClause(fieldValuesMap).toString();
    }

    /**
     * 构造转换等于号多值条件至 "in" 条件，单值保留为 "=" 条件 <br>
     *
     * @param fieldValuesMap 字段值
     * @return where clause string
     */
    private StringBuilder buildEqualClause(Map<String, List<String>> fieldValuesMap) {
        StringBuilder sb = new StringBuilder();
        if (!fieldValuesMap.isEmpty()) {

            boolean first = true;
            for (Map.Entry<String, List<String>> entry : fieldValuesMap.entrySet()) {
                if (!first) {
                    sb.append(" and ");
                }
                first = false;
                sb.append(entry.getKey());
                List<String> values = entry.getValue();
                if (values.size() > 1) {
                    sb.append(" in (");
                    for (String value : values) {
                        sb.append("'").append(value).append("'").append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                    sb.append(")");
                } else {
                    sb.append(" = ").append("'").append(values.get(0)).append("'");
                }
            }
        }

        return sb;
    }

    /**
     * TODO: 对于通过 JDBC 操作的方式，可以把这块逻辑抽出来
     *
     * @param conditionString 条件字段
     * @return where clause
     */
    private String buildWhereClause(String conditionString) {

        if (conditionString == null || conditionString.isEmpty()) {
            return "";
        }

        String[] conditions = conditionString.split(";");

        StringBuilder whereClause = new StringBuilder();
        Pattern pattern = Pattern.compile("^(\\w+)(>=|<=|<>|!=|=|>|<| LIKE | like )(.*)$");


        Map<String, List<String>> equalFieldValuesMap = new HashMap<>();

        for (String condition : conditions) {
            Matcher matcher = pattern.matcher(condition.trim());

            if (!matcher.matches() || matcher.groupCount() != 3) {
                throw new DataproxyException(DataproxyErrorCode.INVALID_PARTITION_SPEC, "Invalid condition format: " + condition);
            }

            String column = matcher.group(1).trim();
            String operator = matcher.group(2);
            String value = matcher.group(3).trim();

            if (!columnOrValuePattern.matcher(column).matches()) {
                throw new DataproxyException(DataproxyErrorCode.INVALID_PARTITION_SPEC, "Invalid condition format: " + column);
            }

            if (!columnOrValuePattern.matcher(value).matches()) {
                throw new DataproxyException(DataproxyErrorCode.INVALID_PARTITION_SPEC, "Invalid condition format: " + column);
            }

            // 安全处理用户输入的值，可以根据具体需要进行处理
            value = value.replace("'", "''"); // 简单处理单引号转义

            if ("=".equals(operator)) {
                if (equalFieldValuesMap.containsKey(column)) {
                    equalFieldValuesMap.get(column).add(value);
                } else {
                    equalFieldValuesMap.put(column, new ArrayList<>(List.of(value)));
                }
            } else {
                if (!whereClause.isEmpty()) {
                    whereClause.append(" and ");
                }
                whereClause.append(column).append(' ').append(operator).append(" '").append(value).append("'");
            }
        }
        StringBuilder equalFieldClause = buildEqualClause(equalFieldValuesMap);

        if (whereClause.isEmpty()) {
            return equalFieldClause.toString();
        }

        if (!equalFieldClause.isEmpty()) {
            whereClause.append(" and ").append(equalFieldClause);
        }
        return whereClause.toString();
    }

    private void toArrowVector(Record record, VectorSchemaRoot root, int rowIndex) throws IOException {
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
                    varcharVector.setSafe(rowIndex, record.getString(columnName).getBytes(StandardCharsets.UTF_8));
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
