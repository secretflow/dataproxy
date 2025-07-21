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
import java.util.LinkedHashMap;
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
    private final BiFunction<Connection, String, Boolean> checkTableExists;
    private final int BATCH_NUM = 500;
    @FunctionalInterface
    public interface BuildCreateTableSqlFunc {
        String apply(String tableName, Schema schema, Map<String, String> partitionSpec);
    }
    private final BuildCreateTableSqlFunc buildCreateTableSql;
    @FunctionalInterface
    public interface BuildInsertSqlFunc {
        String apply(String tableName, Schema schema, Map<String,Object> data, Map<String, String> partitionSpec);
    }
    private BuildInsertSqlFunc buildInsertSql;

    @FunctionalInterface
    public interface BuildMultiInsertSqlFunc {
        String apply(String tableName, Schema schema, List<Map<String,Object>> data, Map<String, String> partitionSpec);
    }
    private BuildMultiInsertSqlFunc buildMultiInsertSql;
    private final Map<String, String> partitionSpec;
    private final String tableName;
    private Connection connection;
    private final boolean supportMutliInsert;

    public static Map<String, String> PasrsePartition(String partition) {
        Map<String, String> res = new LinkedHashMap<>();
        String[] groups = partition.split("[,/]");
        for (String group : groups) {
            String[] kv = group.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }

            String k = kv[0].trim();
            String v = kv[1].trim()
                    .replaceAll("'", "")
                    .replaceAll("\"", "");
            if (k.isEmpty() || v.isEmpty()) {
                throw new IllegalArgumentException("Invalid partition spec.");
            }

            res.put(k, v);
        }
        return res;
    }

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig,
                                Function<DatabaseConnectConfig, Connection> initFunc,
                                BuildCreateTableSqlFunc buildCreateTableSql,
                                BuildInsertSqlFunc buildInsertSql,
                                BiFunction<Connection, String, Boolean> checkTableExists) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.checkTableExists = checkTableExists;
        this.buildCreateTableSql = buildCreateTableSql;
        this.buildInsertSql = buildInsertSql;
        this.tableName = this.dbTableConfig.tableName();
        this.partitionSpec = PasrsePartition(this.dbTableConfig.partition());
        supportMutliInsert = false;
        this.prepare();
    }

    public DatabaseRecordWriter(DatabaseWriteConfig commandConfig,
                                Function<DatabaseConnectConfig, Connection> initFunc,
                                BuildCreateTableSqlFunc buildCreateTableSql,
                                BuildMultiInsertSqlFunc buildMultiInsertSql,
                                BiFunction<Connection, String, Boolean> checkTableExists) {
        this.commandConfig = commandConfig;
        this.dbConnectConfig = commandConfig.getDbConnectConfig();
        this.dbTableConfig = commandConfig.getCommandConfig();
        this.initFunc = initFunc;
        this.checkTableExists = checkTableExists;
        this.buildCreateTableSql = buildCreateTableSql;
        this.buildMultiInsertSql = buildMultiInsertSql;
        this.tableName = this.dbTableConfig.tableName();
        this.partitionSpec = PasrsePartition(this.dbTableConfig.partition());
        supportMutliInsert = true;
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

        preProcessing(dbTableConfig.tableName());

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

        if(supportMutliInsert) {
            List<Map<String, Object>> multiRecords = new ArrayList<>();
            for(int rowIndex = 0; rowIndex < batchSize; rowIndex ++) {
                Record record = new Record();
                for(int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    log.info("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex));
                    columnName = root.getVector(columnIndex).getField().getName().toLowerCase();
                    record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
                }
                multiRecords.add(record.getData());
                if(multiRecords.size() == BATCH_NUM) {
                    this.insertMultiData(commandConfig.getResultSchema(), multiRecords);
                    multiRecords.clear();
                }
            }
        } else {
            for(int rowIndex = 0; rowIndex < batchSize; rowIndex ++) {
                Record record = new Record();
                for(int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    log.info("column: {}, type: {}", columnIndex, root.getFieldVectors().get(columnIndex));
                    columnName = root.getVector(columnIndex).getField().getName().toLowerCase();
                    record.set(columnName, this.getValue(root.getFieldVectors().get(columnIndex), rowIndex));
                }
                this.insertData(commandConfig.getResultSchema(), record.getData());
            }
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

    private void createTable(Schema schema){
        String createTableSql = this.buildCreateTableSql.apply(tableName, schema, partitionSpec);
        try (Statement stmt = connection.createStatement()){
            stmt.executeUpdate(createTableSql);
        } catch (SQLException e) {
            log.error("create table sql:{} error: {}", createTableSql, e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private void dropTable() throws SQLException {
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

    private void deleteAllRowOfTable() throws SQLException {
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

    public void insertData(Schema arrowSchema, Map<String, Object> data) {
        String sql = this.buildInsertSql.apply(tableName, arrowSchema, data, partitionSpec);
        try (PreparedStatement stmt = connection.prepareStatement(sql)){
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("insert data error: sql:\"{}\" error:\"{}\"", sql, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void insertMultiData(Schema arrowSchema, List<Map<String, Object>> multiData){
        String sql = this.buildMultiInsertSql.apply(tableName, arrowSchema, multiData, partitionSpec);
        try (PreparedStatement stmt = connection.prepareStatement(sql)){
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("insert data error: sql:\"{}\" error:\"{}\"", sql, e.getMessage());
            throw new RuntimeException(e);
        }
    }


    // create table when the table not exist
    private void preProcessing(String tableName){
        if(checkTableExists.apply(connection, tableName)) {
            log.info("database table is exists, table name: {}", tableName);
            log.info("trying dropping table {}", tableName);
            try {
                this.dropTable();
            } catch (SQLException e) {
                try {
                    this.deleteAllRowOfTable();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }

        } else {
            log.info("table {} no exists", tableName);
        }
        createTable(commandConfig.getResultSchema());
    }

}
