/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.utils.Record;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class DatabaseRecordWriter implements Writer {
    private final DatabaseCommandConfig<?> commandConfig;

    private final DatabaseConnectConfig dbConnectConfig;
    private final DatabaseTableConfig dbTableConfig;
    private final Function<DatabaseConnectConfig, Connection> initFunc;
    private Connection connection;

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig, Function<DatabaseConnectConfig, Connection> initFunc) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.prepare();
    }

    private Connection initDatabaseClient(DatabaseConnectConfig dbConnectConfig) {
        if(dbConnectConfig == null) {
            throw new IllegalArgumentException("connConfig is null");
        }
        return this.initFunc.apply(dbConnectConfig);
    }
    private void prepare(){

        connection = initDatabaseClient(dbConnectConfig);

        preProcessing(connection, dbTableConfig.tableName());

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

    @Override
    public void write(VectorSchemaRoot root) {
        final int batchSize = root.getRowCount();
        log.info("database writer batchSize: {}", batchSize);
        int columnCount = root.getFieldVectors().size();

        String columnName;
        Record record = new Record();
        for(int rowIndex = 0; rowIndex < batchSize; rowIndex ++) {
            for(int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                log.debug("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex));
                columnName = root.getVector(columnIndex).getField().getName().toLowerCase();

                record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
            }
            try{
                this.insertData(connection, commandConfig.getResultSchema(), dbTableConfig.tableName(), record.getData());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            log.debug("record: {}", record);
        }
    }

    @Override
    public void flush() {

    }

    public void close() {
        try{
            connection.close();
        } catch (SQLException e) {
            log.error("database connection close error");
            throw new RuntimeException(e);
        }

    }
    // 根据Arrow字段类型转换为JDBC类型
    private static String getJdbcType(Field field) {
        return switch (field.getFieldType().getType().getTypeID()) {
            case Int -> "INT";
            case Utf8 -> "VARCHAR(255)";
            case FloatingPoint -> "FLOAT";
            default -> "VARCHAR(255)"; // 默认使用VARCHAR
        };
    }

    private void createTableFromSchema(Connection connection,Schema schema, String tableName){

        StringBuilder createTableSql = new StringBuilder("CREATE TABLE \""+ tableName + "\" (");
        for (Field field : schema.getFields()) {
            createTableSql.append("\n   ");
            createTableSql.append(field.getName());
            createTableSql.append(" ");
            createTableSql.append(getJdbcType(field));
            createTableSql.append(",");
        }
        createTableSql.setCharAt(createTableSql.length() - 1, ')');
        try{
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(createTableSql.toString());
        } catch (SQLException e) {
            log.error("create table error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

    // 假设传入的 Arrow Schema 和列名对应的 Object 数据
    public void insertData(Connection conn, Schema arrowSchema, String tableName, Map<String, Object> data) throws SQLException {
        // 构建 SQL 插入语句
        StringBuilder sql = new StringBuilder("INSERT INTO \""+ tableName +"\" (");
        StringBuilder values = new StringBuilder("VALUES (");

        // 获取 Schema 中的所有列
        List<Field> fields = arrowSchema.getFields();

        // 构建列名部分和 VALUES 部分
        List<Object> valueList = new ArrayList<>();
        for (Field field : fields) {
            String columnName = field.getName();
            sql.append(columnName).append(", ");

            // 获取该列的值
            Object value = data.get(columnName);
            if (value == null) {
                values.append("NULL, ");
            } else {
                values.append("?, ");
                valueList.add(value);  // 将数据值添加到 valueList
            }
        }

        // 去掉最后的 ", " 并关闭括号
        sql.setLength(sql.length() - 2);
        sql.append(") ");

        values.setLength(values.length() - 2);
        values.append(")");

        // 完成 SQL 插入语句
        sql.append(values);

        // 执行插入操作
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            // 设置参数
            int index = 1;
            for (Object value : valueList) {
                setStatementParameter(stmt, index++, value);
            }

            stmt.executeUpdate();
        }
    }

    // 根据不同的列类型，将 Object 转换为合适的 JDBC 数据类型
    private static void setStatementParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            stmt.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else {
            stmt.setObject(index, value);  // 默认使用 setObject
        }
    }

    // 如果表格不存在，创建表格
    private void preProcessing(Connection connection, String tableName){
        if(!isExistsTable(connection, tableName)) {
            log.info("database table is not exists, create table successful, table name: {}", tableName);
            createTableFromSchema(connection, commandConfig.getResultSchema(), tableName);
        } else {
            log.info("database table is exists, table name: {}", tableName);
        }
    }

    private boolean isExistsTable(Connection connection, String tableName){
        try{
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"}); {
                return resultSet.next();
            }
        } catch(SQLException e) {
            log.error("check whether table has existed error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

}
