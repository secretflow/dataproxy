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

package org.secretflow.dataproxy.plugin.database.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.*;

import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.v1alpha1.common.Common;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;


@Slf4j
public class DatabaseDoGetContext {

    private final DatabaseCommandConfig<?> dbCommandConfig;

    @Getter
    private Schema schema;

    @Getter
    private ResultSet resultSet;

    private Statement queryStmt;
    private Connection conn;
    @Getter
    private DatabaseMetaData databaseMetaData;

    @Getter
    private String tableName;

    private final Map<byte[], ParamWrapper> ticketWrapperMap = new ConcurrentHashMap<>();
    private final ReadWriteLock tickerWrapperMapRwLock = new ReentrantReadWriteLock();

    private final Function<DatabaseConnectConfig, Connection> initDatabaseFunc;

    @FunctionalInterface
    public interface BuildQuerySqlFunc<T, U, V, String> {
        String apply(T t, U u, V v);
    }
    private final BuildQuerySqlFunc<String, List<String>, String, String>  buildQuerySqlFunc;

    private final Function<String, ArrowType> jdbcType2ArrowType;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();


    public DatabaseDoGetContext(DatabaseCommandConfig<?> config, Function<DatabaseConnectConfig, Connection> initDatabaseFunc, BuildQuerySqlFunc<String, List<String>, String, String> buildQuerySqlFunc, Function<String, ArrowType> jdbcType2ArrowType) {
        this.dbCommandConfig = config;
        this.initDatabaseFunc = initDatabaseFunc;
        this.buildQuerySqlFunc = buildQuerySqlFunc;
        this.jdbcType2ArrowType = jdbcType2ArrowType;
        prepare();
    }

    public List<TaskConfig> getTaskConfigs() {
        return Collections.singletonList(new TaskConfig(this, 0));
    }

    private void prepare(){
        DatabaseConnectConfig dbConnectConfig = dbCommandConfig.getDbConnectConfig();

        String querySql;

        conn = this.initDatabaseFunc.apply(dbConnectConfig);
        if (dbCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
            querySql = scqlReadJobConfig.getCommandConfig();
        } else if (dbCommandConfig instanceof DatabaseTableQueryConfig dbTableQueryConfig) {
            DatabaseTableConfig tableConfig = dbTableQueryConfig.getCommandConfig();
            this.tableName = tableConfig.tableName();
            querySql = this.buildQuerySqlFunc.apply(this.tableName, tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), tableConfig.partition());
            this.schema = dbCommandConfig.getResultSchema();
        } else {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported read parameter type: " + dbCommandConfig.getClass());
        }
        this.executeSqlTaskAndHandleResult(conn, this.tableName, querySql);
    }

    private void executeSqlTaskAndHandleResult(Connection connection, String tableName, String querySql) {
        log.info("database execute sql: {}", querySql);
        Throwable throwable = null;
        try {
            readWriteLock.writeLock().lock();
            this.databaseMetaData = connection.getMetaData();
            queryStmt = connection.createStatement();
            resultSet = queryStmt.executeQuery(querySql);
            if (dbCommandConfig.getDbTypeEnum() == DatabaseTypeEnum.SQL) {
                this.initArrowSchemaFromColumns(connection.getMetaData(), tableName);
            }

        } catch (SQLException e) {
            log.error("sql execute error", e);
            throwable = e;
            throw DataproxyException.of(DataproxyErrorCode.DATABASE_ERROR, e.getMessage(), e);
        } catch (Exception e) {
            throwable = e;
            throw DataproxyException.of(DataproxyErrorCode.DATABASE_ERROR, "database execute sql error", e);
        } finally {
            readWriteLock.writeLock().unlock();
            loadLazyConfig(throwable);
        }
    }

    public void close() {
        try{
            queryStmt.close();
            resultSet.close();
            conn.close();
        } catch (SQLException e) {
            log.error("query jdbc close error: {}", e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private void initArrowSchemaFromColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        List<Field> fields = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");

            ArrowType arrowType = this.jdbcType2ArrowType.apply(columnType);
            Field field = new Field(columnName, FieldType.nullable(arrowType), null);
            fields.add(field);
        }
        columns.close();
        schema = new Schema(fields);
    }


    private void loadLazyConfig(Throwable throwable) {
        tickerWrapperMapRwLock.writeLock().lock();
        try {
            if (ticketWrapperMap.isEmpty()) {
                return;
            }
            List<TaskConfig> taskConfigs = getTaskConfigs();
            if (taskConfigs.isEmpty()) {
                throw new IllegalArgumentException("#getTaskConfigs is empty");
            }

            log.info("config list size: {}", taskConfigs.size());
            log.info("ticketWrapperMap size: {}", ticketWrapperMap.size());

            int index = 0;
            ParamWrapper paramWrapper;
            for (Map.Entry<byte[], ParamWrapper> entry : ticketWrapperMap.entrySet()) {
                paramWrapper = entry.getValue();

                if (index < taskConfigs.size()) {
                    TaskConfig taskConfig = taskConfigs.get(index);
                    taskConfig.setError(throwable);
                    log.info("Load lazy taskConfig: {}", JsonUtils.toString(taskConfig));
                    paramWrapper.setParamIfAbsent(taskConfig);
                } else {
                    log.info("Set the remaining ticketWrapperMap to a default TaskConfig that doesn't read data. index: {},", index);
                    TaskConfig taskConfig = new TaskConfig(this, 0);
                    taskConfig.setError(throwable);
                    paramWrapper.setParamIfAbsent(taskConfig);
                }
                index++;
            }
        } finally {
            tickerWrapperMapRwLock.writeLock().unlock();
        }
    }

}
