/*
 * Copyright 2023 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.manager.connector.rdbms;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.format.*;
import org.secretflow.dataproxy.common.model.datasource.DatasourceTypeEnum;
import org.secretflow.dataproxy.common.model.datasource.conn.JdbcBaseConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.JdbcLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.location.LocationConfig;
import org.secretflow.dataproxy.common.utils.IdUtils;
import org.secretflow.dataproxy.manager.Connector;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.DataWriter;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfig;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfigBuilder;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author muhong
 * @date 2023-09-07 13:47
 */
public class JdbcConnector implements Connector {

    protected JdbcBaseConnConfig connConfig;

    protected HikariDataSource dataSource;

    protected JdbcAssistant jdbcAssistant;

    public JdbcConnector() {
    }

    public JdbcConnector(DatasourceTypeEnum type, JdbcBaseConnConfig connConfig) {
        // 构造jdbc辅助类
        switch (type) {
            case MYSQL:
                this.jdbcAssistant = new MysqlJdbcAssistant();
                break;
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的jdbc数据源类型 " + type);
        }

        HikariConfig config = new HikariConfig();
        config.setUsername(connConfig.getUserName());
        config.setPassword(connConfig.getPassword());
        config.setDriverClassName(this.jdbcAssistant.getDriverClass());
        config.setConnectionTestQuery(this.jdbcAssistant.getConnectionTestQuery());
        config.setMaximumPoolSize(connConfig.getMaximumPoolSize());
        config.setMinimumIdle(connConfig.getMinimumIdle());
        config.addDataSourceProperty("cachePrepStmts", connConfig.getCachePrepStmts());
        config.addDataSourceProperty("useServerPrepStmts", connConfig.getUseServerPrepStmts());
        config.addDataSourceProperty("prepStmtCacheSize", connConfig.getPrepStmtCacheSize());
        config.addDataSourceProperty("prepStmtCacheSqlLimit", connConfig.getPrepStmtCacheSqlLimit());
        config.addDataSourceProperty("allowLoadLocalInfile", "false");
        config.addDataSourceProperty("allowUrlInLocalInfile", "false");
        config.addDataSourceProperty("allowLoadLocalInfileInPath", "");
        config.addDataSourceProperty("autoDeserialize", "false");

        // 不同数据库对 catalog 和 schema 的使用方法不同，所以交给子类处理
        this.jdbcAssistant.initDataSourceConfig(config, connConfig);
        this.connConfig = connConfig;
        try {
            dataSource = new HikariDataSource(config);
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.CREATE_DATASOURCE_CONNECTOR_ERROR, e.getMessage(), e);
        }
        checkAdaptorStatus();
    }

    /**
     * 获取真实字段名
     *
     * @param viewFieldName 展示列名
     * @param fieldMap      真实列名-展示列名映射
     * @return 真实列名
     */
    public static String getRawFieldName(String viewFieldName, Map<String, String> fieldMap) {
        // 若列名为空或映射关系为空，直接返回
        if (StringUtils.isEmpty(viewFieldName) || MapUtils.isEmpty(fieldMap)) {
            return viewFieldName;
        }

        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            if (entry.getValue().equals(viewFieldName)) {
                return entry.getKey();
            }
        }

        // 映射关系中没有的，直接展示列名
        return viewFieldName;
    }

    @Override
    public InferSchemaResult inferSchema(BufferAllocator allocator, LocationConfig locationConfig, DatasetFormatConfig formatConfig) {
        this.jdbcAssistant.fillDefaultValue(connConfig, (JdbcLocationConfig) locationConfig);
        String table = ((JdbcLocationConfig) locationConfig).getTable();

        TableFormatConfig reqFormatConfig = (TableFormatConfig) formatConfig.getFormatConfig();
        TableFormatConfig resultFormatConfig = reqFormatConfig == null ? TableFormatConfig.builder().build() : reqFormatConfig.toBuilder().build();

        try (Connection conn = this.jdbcAssistant.getDatabaseConn(this.dataSource)) {

            // schema推断
            Schema schema = getSchema(allocator, conn, this.jdbcAssistant.composeTableName((JdbcLocationConfig) locationConfig));

            // 原始数据字段集合
            Set<String> fieldNameSet = schema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
            Set<String> viewFieldNameSet = new HashSet<>();

            // 字段映射检查
            if (MapUtils.isNotEmpty(resultFormatConfig.getFieldMap())) {
                resultFormatConfig.getFieldMap().forEach((rawFieldName, viewFieldName) -> {
                    if (!fieldNameSet.contains(rawFieldName)) {
                        throw DataproxyException.of(DataproxyErrorCode.FIELD_NOT_EXIST, "映射字段 " + rawFieldName);
                    }
                    if (viewFieldNameSet.contains(viewFieldName)) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "字段 " + viewFieldName + " 重复");
                    }
                    viewFieldNameSet.add(viewFieldName);
                });

                fieldNameSet.stream()
                        .filter(fieldName -> !resultFormatConfig.getFieldMap().containsKey(fieldName))
                        .forEach(fieldName -> {
                            if (viewFieldNameSet.contains(fieldName)) {
                                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "字段 " + fieldName + " 重复");
                            }
                            viewFieldNameSet.add(fieldName);
                            resultFormatConfig.getFieldMap().put(fieldName, fieldName);
                        });
            } else {
                resultFormatConfig.setFieldMap(fieldNameSet.stream().collect(Collectors.toMap(name -> name, name -> name)));
                viewFieldNameSet.addAll(fieldNameSet);
            }

            // 主键推断与校验
            if (StringUtils.isNotEmpty(resultFormatConfig.getPrimaryKey())) {
                // 如果用户填写参数中已经包含主键，则校验字段存在性
                if (!fieldNameSet.contains(getRawFieldName(resultFormatConfig.getPrimaryKey(), resultFormatConfig.getFieldMap()))) {
                    throw DataproxyException.of(DataproxyErrorCode.FIELD_NOT_EXIST, "主键 " + resultFormatConfig.getPrimaryKey());
                }
            } else {
                String primaryKey = getPrimaryKeyColumnName(conn, table);
                resultFormatConfig.setPrimaryKey(primaryKey);
            }

            // 索引推断与校验
//            if (CollectionUtils.isNotEmpty(resultFormatConfig.getIndex())) {
//                resultFormatConfig.getIndex().forEach(index -> {
//                    if (CollectionUtils.isEmpty(index.getField())) {
//                        throw FastDFException.of(DataProxyErrorCode.PARAMS_UNRELIABLE, "索引 " + index.getIndexName() + " 字段为空");
//                    }
//                    if (!viewFieldNameSet.containsAll(index.getField())) {
//                        throw FastDFException.of(DataProxyErrorCode.PARAMS_UNRELIABLE, "索引 " + index.getIndexName() + " 字段不存在");
//                    }
//                });
//            } else {
//                // 查询索引
//                List<TableIndex> indexList = getIndex(conn, table);
//                resultFormatConfig.setIndex(indexList);
//            }

            // 原先就填写了分页字段，需要做严格校验
            if (resultFormatConfig.getPartitionBehavior() != null && StringUtils.isNotEmpty(resultFormatConfig.getPartitionBehavior().getFieldName())) {
                String partitionFieldName = resultFormatConfig.getPartitionBehavior().getFieldName();
                if (!viewFieldNameSet.contains(partitionFieldName)) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "分页字段 " + partitionFieldName + " 不存在");
                }
                ArrowType.ArrowTypeID partitionFieldTypeId = schema.findField(partitionFieldName).getFieldType().getType().getTypeID();

                // 当且仅当主键为整型、浮点型、时间等能做数值计算时，可以作为分页条件
                if (!(ArrowType.ArrowTypeID.Int.equals(partitionFieldTypeId)
                        || ArrowType.ArrowTypeID.Decimal.equals(partitionFieldTypeId)
                        || ArrowType.ArrowTypeID.FloatingPoint.equals(partitionFieldTypeId)
                        || ArrowType.ArrowTypeID.Date.equals(partitionFieldTypeId))) {
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "分页字段 " + partitionFieldName + " 无法做数值计算");
                }
                resultFormatConfig.getPartitionBehavior().setType(partitionFieldTypeId);
            } else if (StringUtils.isNotEmpty(resultFormatConfig.getPrimaryKey())) {
                // 原先没有填写分页参数，尝试推导
                // 若未指定分页参数，尝试用主键作为分页条件
                ArrowType.ArrowTypeID primaryKeyTypeId = schema.findField(resultFormatConfig.getPrimaryKey()).getFieldType().getType().getTypeID();
                if (ArrowType.ArrowTypeID.Int.equals(primaryKeyTypeId)
                        || ArrowType.ArrowTypeID.Decimal.equals(primaryKeyTypeId)
                        || ArrowType.ArrowTypeID.FloatingPoint.equals(primaryKeyTypeId)
                        || ArrowType.ArrowTypeID.Date.equals(primaryKeyTypeId)) {
                    resultFormatConfig.setPartitionBehavior(PartitionBehavior.builder()
                            .fieldName(resultFormatConfig.getPrimaryKey())
                            .type(primaryKeyTypeId)
                            .build());
                }
            } else {
                resultFormatConfig.setPartitionBehavior(null);
            }

            return InferSchemaResult.builder()
                    .schema(schema)
                    .datasetFormatConfig(DatasetFormatConfig.builder()
                            .type(DatasetFormatTypeEnum.TABLE)
                            .formatConfig(resultFormatConfig)
                            .build())
                    .build();
        } catch (SQLException e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_CALL_ERROR, "表结构推断失败", e);
        }
    }

    @Override
    public DataReader buildReader(BufferAllocator allocator, DatasetReadCommand readCommand) {
        JdbcLocationConfig jdbcLocationConfig = (JdbcLocationConfig) readCommand.getLocationConfig().getLocationConfig();
        this.jdbcAssistant.fillDefaultValue(connConfig, jdbcLocationConfig);

        // 列名缺省处理
        if (CollectionUtils.isEmpty(readCommand.getFieldList())) {
            readCommand.setFieldList(readCommand.getSchema().getFields().stream()
                    .map(Field::getName)
                    .collect(Collectors.toList())
            );
        }

        return new JdbcDataReader(allocator,
                this.jdbcAssistant,
                this.dataSource,
                (TableFormatConfig) readCommand.getFormatConfig().getFormatConfig(),
                readCommand.getOutputFormatConfig(),
                this.jdbcAssistant.composeTableName(jdbcLocationConfig),
                readCommand.getSchema(),
                readCommand.getFieldList(),
                readCommand.getFilter());
    }

    @Override
    public DataWriter buildWriter(DatasetWriteCommand writeCommand) {
        JdbcLocationConfig jdbcLocationConfig = (JdbcLocationConfig) writeCommand.getLocationConfig().getLocationConfig();
        this.jdbcAssistant.fillDefaultValue(connConfig, jdbcLocationConfig);

        if (writeCommand.getFormatConfig().getFormatConfig() == null) {
            writeCommand.getFormatConfig().setFormatConfig(TableFormatConfig.builder().build());
        }
        TableFormatConfig formatConfig = (TableFormatConfig) writeCommand.getFormatConfig().getFormatConfig();

        // 未定义主键，需要补充一个
        if (StringUtils.isEmpty(formatConfig.getPrimaryKey())) {
            String primaryKey = "pk_" + IdUtils.createRandString(6);
            formatConfig.setPrimaryKey(primaryKey);
        }
        return new JdbcDataWriter(this.jdbcAssistant,
                this.dataSource,
                this.jdbcAssistant.composeTableName(jdbcLocationConfig), jdbcLocationConfig,
                formatConfig,
                writeCommand.getSchema());
    }

    @Override
    public boolean isAvailable() {
        try {
            checkAdaptorStatus();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 连通性测试
     */
    public void checkAdaptorStatus() {
        try (Connection conn = this.jdbcAssistant.getDatabaseConn(dataSource);
             PreparedStatement preparedStatement = conn.prepareStatement(this.jdbcAssistant.getConnectionTestQuery())) {
            preparedStatement.execute();
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_DATASOURCE_CONNECTION_VALIDATE_FAILED, e);
        }
    }

    protected Schema getSchema(BufferAllocator allocator, Connection conn, String tableName) throws SQLException {
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put("sqlPart", "* from " + tableName);
        valuesMap.put("limitPart", String.format(this.jdbcAssistant.generateLimitConditionTemplate(false), 1));
        String sampleSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());
        try (ResultSet sampleRs = conn.createStatement().executeQuery(sampleSql)) {
            // 设置浮点型精度规则，四舍五入，保证精度有损场景
            JdbcToArrowConfig config = new JdbcToArrowConfigBuilder()
                    .setAllocator(allocator)
                    .setBigDecimalRoundingMode(RoundingMode.CEILING)
                    .build();
            // schema推断
            return JdbcToArrowUtils.jdbcToArrowSchema(sampleRs.getMetaData(), config);
        }
    }

    /**
     * 获取主键名称<br/>
     * <p>
     * hive数据库不需要
     *
     * @param conn      数据库连接
     * @param tableName 表名
     * @return 主键名称
     * @throws SQLException
     */
    protected String getPrimaryKeyColumnName(Connection conn, String tableName) throws SQLException {
        String primaryKeyColumnName;
        try (ResultSet primaryKeyResultSet = conn.getMetaData().getPrimaryKeys(
                dataSource.getCatalog(), dataSource.getSchema(), tableName
        )) {
            if (!primaryKeyResultSet.next()) {
                return null;
            }
            primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
            if (StringUtils.isEmpty(primaryKeyColumnName)) {
                return null;
            }
        }
        return primaryKeyColumnName;
    }

    /**
     * 索引查询
     *
     * @param conn      数据库连接
     * @param tableName 表名
     * @return
     * @throws SQLException
     */
    protected List<TableIndex> getIndex(Connection conn, String tableName) throws SQLException {
        Map<String, TableIndex> indexMap = new HashMap<>();

        try (ResultSet indexResultSet = conn.getMetaData().getIndexInfo(
                dataSource.getCatalog(), dataSource.getSchema(), tableName, false, false
        )) {
            while (indexResultSet.next()) {
                String colName = indexResultSet.getString("COLUMN_NAME");
                String indexName = indexResultSet.getString("INDEX_NAME");

                if (!indexMap.containsKey(indexName)) {
                    indexMap.put(indexName, TableIndex.builder()
                            .indexName(indexName)
                            .type(indexResultSet.getBoolean("NON_UNIQUE") ? IndexType.INDEX : IndexType.UNIQUE)
                            .field(new ArrayList<>())
                            .build());
                }
                TableIndex index = indexMap.get(indexName);
                index.getField().add(colName);
            }
        }
        return new ArrayList<>(indexMap.values());
    }

    @Override
    public void close() throws Exception {
        this.dataSource.close();
    }
}