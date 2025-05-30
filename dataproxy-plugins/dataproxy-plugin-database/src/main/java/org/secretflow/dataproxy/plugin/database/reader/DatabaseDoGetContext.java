package org.secretflow.dataproxy.plugin.database.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.Types;
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
import java.util.function.Function;
import java.util.regex.Pattern;

@Slf4j
public class DatabaseDoGetContext {

    private final DatabaseCommandConfig<?> dbCommandConfig;

    @Getter
    private long count;

    @Getter
    private Schema schema;

    @Getter
    private ResultSet resultSet;

    @Getter
    private DatabaseMetaData databaseMetaData;

    @Getter
    private String tableName;

    private final Map<byte[], ParamWrapper> ticketWrapperMap = new ConcurrentHashMap<>();
    private final ReadWriteLock tickerWrapperMapRwLock = new ReentrantReadWriteLock();

    private final Function<DatabaseConnectConfig, Connection> initDatabase;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public DatabaseDoGetContext(DatabaseCommandConfig<?> config, Function<DatabaseConnectConfig, Connection> initDatabase) {
        this.dbCommandConfig = config;
        this.initDatabase = initDatabase;
        prepare();
    }

    public List<TaskConfig> getTaskConfigs() {
        return Collections.singletonList(new TaskConfig(this, 0, count));
    }

    private void prepare(){
        DatabaseConnectConfig dbConnectConfig = dbCommandConfig.getDbConnectConfig();

        String querySql;
        Connection conn;
        try {
            conn = this.initDatabase.apply(dbConnectConfig);
            if (dbCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
                querySql = scqlReadJobConfig.getCommandConfig();
            } else if (dbCommandConfig instanceof DatabaseTableQueryConfig dbTableQueryConfig) {
                DatabaseTableConfig tableConfig = dbTableQueryConfig.getCommandConfig();
                this.tableName = tableConfig.tableName();
                querySql = this.buildSql(this.tableName, tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), tableConfig.partition());
                this.schema = dbCommandConfig.getResultSchema();
            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported read parameter type: " + dbCommandConfig.getClass());
            }
            this.executeSqlTaskAndHandleResult(conn, this.tableName, querySql);
            conn.close();
        } catch (SQLException e) {
            log.error("sql execute error", e);
            throw new RuntimeException(e);
        }


    }

    private void executeSqlTaskAndHandleResult(Connection connection, String tableName, String querySql) {
        log.info("database execute sql: {}", querySql);
        Throwable throwable = null;
        try {
            readWriteLock.writeLock().lock();
            this.databaseMetaData = connection.getMetaData();
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(querySql);
            if (dbCommandConfig.getDbTypeEnum() == DatabaseTypeEnum.SQL) {
                this.initArrowSchemaFromColumns(connection.getMetaData(), tableName);
            }
            stmt.close();

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

    private static ArrowType getArrowType(String jdbcType) {
        switch (jdbcType.toLowerCase()) {
            case "int":
            case "integer":
                return Types.MinorType.INT.getType();
            case "bigint":
                return Types.MinorType.BIGINT.getType();
            case "float":
                return Types.MinorType.FLOAT4.getType();
            case "double":
                return Types.MinorType.FLOAT8.getType();
            case "varchar":
            case "string":
                Types.MinorType.VARCHAR.getType();
            case "boolean":
                Types.MinorType.BIT.getType();
            case "date":
                Types.MinorType.DATEDAY.getType();
            case "timestamp":
                Types.MinorType.TIMESTAMPMILLI.getType();
            default:
                throw new IllegalArgumentException("Unsupported JDBC type: " + jdbcType);
        }
    }

    private void initArrowSchemaFromColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        List<Field> fields = new ArrayList<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");

            ArrowType arrowType = getArrowType(columnType);
            Field field = new Field(columnName, FieldType.nullable(arrowType), null);
            fields.add(field);
        }
        schema = new Schema(fields);
    }

    private String buildSql(String tableName, List<String> fields, String whereClause) {
        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        log.info("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from " + tableName;
    }

    private void loadLazyConfig(Throwable throwable) {
        tickerWrapperMapRwLock.writeLock().lock();
        try {
            // 1. check if the ticketWrapperMap is empty
            if (ticketWrapperMap.isEmpty()) {
                // If the ticketWrapperMap is empty, will generate task config when get flight info request
                return;
            }
            // If the ticketWrapperMap is not empty, will set taskConfig from result to ParamWrapper
            // 2. iterate through the ticketWrapperMap and set the taskConfig from result
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
                    // Set the remaining ticketWrapperMap to a default TaskConfig that doesn't read data
                    log.info("Set the remaining ticketWrapperMap to a default TaskConfig that doesn't read data. index: {},", index);
                    TaskConfig taskConfig = new TaskConfig(this, 0, 0);
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
