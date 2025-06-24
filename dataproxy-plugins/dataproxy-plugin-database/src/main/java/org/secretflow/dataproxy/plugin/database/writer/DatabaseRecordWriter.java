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
import org.apache.hive.hplsql.Conn;
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
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class DatabaseRecordWriter implements Writer {
    private final DatabaseCommandConfig<?> commandConfig;

    private final DatabaseConnectConfig dbConnectConfig;
    private final DatabaseTableConfig dbTableConfig;
    private final Function<DatabaseConnectConfig, Connection> initFunc;
    private final Function<String, String> wrapTableName;
    private final Function<Field, String> arrowField2JdbcType;
    private final BiFunction<Connection, String, Boolean> checkTableExists;
    private Connection connection;

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig, Function<DatabaseConnectConfig, Connection> initFunc, Function<String, String> wrapTableName, Function<Field, String> arrowField2JdbcType, BiFunction<Connection, String, Boolean> checkTableExists) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.wrapTableName = wrapTableName;
        this.arrowField2JdbcType = arrowField2JdbcType;
        this.checkTableExists = checkTableExists;
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
                log.info("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex));
                columnName = root.getVector(columnIndex).getField().getName().toLowerCase();

                record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
            }

            this.insertData(connection, commandConfig.getResultSchema(), dbTableConfig.tableName(), record.getData());
            log.info("record: {}", record);
        }
    }

    @Override
    public void flush() {
        // do nothing
    }

    public void close() {
        try{
            connection.close();
        } catch (SQLException e) {
            log.error("database connection close error");
            throw new RuntimeException(e);
        }
    }


    private void createTableFromSchema(Connection connection,Schema schema, String tableName){

        StringBuilder createTableSql = new StringBuilder("CREATE TABLE "+ wrapTableName.apply(tableName) + " (");
        for (Field field : schema.getFields()) {
            createTableSql.append("\n   ");
            createTableSql.append(field.getName());
            createTableSql.append(" ");
            createTableSql.append(arrowField2JdbcType.apply(field));
            createTableSql.append(",");
        }
        createTableSql.setCharAt(createTableSql.length() - 1, ')');
        try{
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(createTableSql.toString());
            stmt.close();
        } catch (SQLException e) {
            log.error("create table sql:{} error: {}", createTableSql, e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private void dropTable(Connection connection, String tableName) throws SQLException {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        String sql = "DROP TABLE IF EXISTS " + tableName;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            log.info("Table {} dropped successfully.", tableName);
        } catch (SQLException e) {
            log.error("Failed to drop table {}: {}", tableName, e.getMessage());
            throw e;
        }
    }

    private void deleteAllRowOfTable(Connection connection, String tableName) throws SQLException {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        String sql = "DELETE FROM " + tableName;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int rowsDeleted = preparedStatement.executeUpdate();
            log.info("Number of rows deleted: {}", rowsDeleted);
        } catch (SQLException e) {
            log.info("Failed to delete data from table {} : {}", tableName, e.getMessage());
            throw e;
        }
    }

    public void insertData(Connection conn, Schema arrowSchema, String tableName, Map<String, Object> data) {
        StringBuilder sql = new StringBuilder("INSERT INTO "+ wrapTableName.apply(tableName) +" (");
        StringBuilder values = new StringBuilder("VALUES (");

        List<Field> fields = arrowSchema.getFields();

        List<Object> valueList = new ArrayList<>();
        for (Field field : fields) {
            String columnName = field.getName();
            sql.append(columnName).append(", ");

            Object value = data.get(columnName);
            if (value == null) {
                values.append("NULL, ");
            } else {
                values.append("?, ");
                valueList.add(value);
            }
        }

        sql.setLength(sql.length() - 2);
        sql.append(") ");

        values.setLength(values.length() - 2);
        values.append(")");

        sql.append(values);
        try {
            PreparedStatement stmt = conn.prepareStatement(sql.toString());
            int index = 1;
            for (Object value : valueList) {
                setStatementParameter(stmt, index++, value);
            }

            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            log.error("insert data error: sql:\"{}\" error:\"{}\"", sql, e.getMessage());
            throw new RuntimeException(e);
        }
    }

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
            stmt.setObject(index, value);
        }
    }

    // create table when the table not exist
    private void preProcessing(Connection connection, String tableName){
        if(checkTableExists.apply(connection, tableName)) {
            log.info("database table is exists, table name: {}", tableName);
            log.info("trying dropping table {}", tableName);
            try {
                dropTable(connection, tableName);
            } catch (SQLException e) {
                try {
                    deleteAllRowOfTable(connection, tableName);
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }

        } else {
            log.info("table {} no exists", tableName);
        }

        createTableFromSchema(connection, commandConfig.getResultSchema(), tableName);
    }


}
