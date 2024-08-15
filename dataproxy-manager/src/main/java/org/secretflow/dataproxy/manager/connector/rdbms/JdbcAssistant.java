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
import org.secretflow.dataproxy.common.model.dataset.format.IndexType;
import org.secretflow.dataproxy.common.model.dataset.format.TableFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.format.TableIndex;
import org.secretflow.dataproxy.common.model.datasource.conn.JdbcBaseConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.JdbcLocationConfig;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.binder.ColumnBinder;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.JDBCType;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * jdbc辅助类
 *
 * @author muhong
 * @date 2023-09-07 19:55
 */
public interface JdbcAssistant<C extends JdbcBaseConnConfig, L extends JdbcLocationConfig> {

    /**
     * 获取连接校验查询语句
     *
     * @return
     */
    String getConnectionTestQuery();

    /**
     * 获取连接驱动类
     *
     * @return
     */
    String getDriverClass();

    /**
     * 初始化 datasource 的 url、catalog 和 schema 配置
     *
     * @param config     datasource hikari config
     * @param connConfig jdbc 连接参数
     */
    void initDataSourceConfig(HikariConfig config, C connConfig);

    default void fillDefaultValue(C connConfig, L locationConfig) {
    }

    /**
     * 装饰标识符，如 mysql 将在标识符前后加上 `<br/>
     * 标识符包括: database, table, index, column, alias, view, stored procedure, partition,
     * tablespace, resource group and other object
     *
     * @param identifier 装饰前的标识符
     * @return 装饰后的标识符
     */
    String decorateIdentifier(String identifier);

    default String decorateStrValue(String value) {
        return "'" + value + "'";
    }

    /**
     * 组装 tableName，如 DB2 将改变 tableName 为 schemaName.tableName
     *
     * @param locationConfig
     * @return 组装后的 tableName
     */
    String composeTableName(L locationConfig);

    /**
     * 是否支持PreparedStatement批量插入
     *
     * @return
     */
    default boolean supportBatchInsert() {
        return true;
    }

    /**
     * 生成列查询语句
     *
     * @param rawFieldName
     * @param composeTableName
     * @return
     */
    default String createFieldPart(List<String> rawFieldName, String composeTableName) {
        List<String> requestedColumnNameList = rawFieldName.stream()
            .map(this::decorateIdentifier)
            .collect(Collectors.toList());
        StringBuilder selectSqlBuilder = new StringBuilder();
        selectSqlBuilder.append(StringUtils.join(requestedColumnNameList, ", "));
        selectSqlBuilder.append(" from ").append(composeTableName).append(" ");
        return selectSqlBuilder.toString();
    }

    /**
     * 查询SQL模板
     *
     * @return 查询SQL模板
     */
    default String selectSQLTemplate() {
        return "select ${sqlPart} ${limitPart}";
    }

    default String generateLimitConditionTemplate(boolean otherFilter) {
        return "limit %s";
    }

    /**
     * 建表SQL
     *
     * @param schema       arrow数据格式
     * @param formatConfig 数据格式配置
     * @return SQL
     */
    default String createTableSql(String composeTableName, Schema schema, TableFormatConfig formatConfig) {
        return "CREATE TABLE " + composeTableName + " ("
            + createTableColumnTypes(schema.getFields(), formatConfig)
            + "," + decorateIdentifier(formatConfig.getPrimaryKey())
            + " BIGINT PRIMARY KEY NOT NULL AUTO_INCREMENT"
            + createIndex(formatConfig.getIndexList(), schema.getFields())
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }

    /**
     * 在目标数据库里预先执行 SQL 的列表
     *
     * @param composeTableName 列定义对象列表
     * @param schema           数据结构
     * @param formatConfig
     * @return SQL 列表
     */
    List<String> preWorkSqls(String composeTableName, Schema schema, L locationConfig, TableFormatConfig formatConfig);

    /**
     * 生成drop table sql 语句
     *
     * @param composeTableName 组合后完整的表名
     * @return sql
     */
    default String dropTableSql(String composeTableName) {
        return "DROP TABLE IF EXISTS " + composeTableName;
    }

    /**
     * 创建列定义sql
     *
     * @param fields 列定义对象列表
     * @return 列定义sql
     */
    default String createTableColumnTypes(List<Field> fields, TableFormatConfig formatConfig) {
        return fields.stream()
            .filter(field -> !field.getName().equals(formatConfig.getPrimaryKey()))
            .map(this::arrowFieldToSqlColumnDefinition)
            .collect(Collectors.joining(","));
    }

    /**
     * 单列定义sql
     *
     * @param field 列定义对象
     * @return sql
     */
    default String arrowFieldToSqlColumnDefinition(Field field) {
        return decorateIdentifier(field.getName()) + " " + jdbcTypeToDbTypeString(arrowTypeToJdbcType(field));
    }

    /**
     * arrow field 转 jdbctype
     *
     * @param field
     * @return
     */
    default JDBCType arrowTypeToJdbcType(Field field) {
        // 定义一个临时vector
        try (BufferAllocator tempAllocator = new RootAllocator();
             FieldVector tempVector = field.createVector(tempAllocator)) {
            ColumnBinder columnBinder = ColumnBinder.forVector(tempVector);
            return JDBCType.valueOf(columnBinder.getJdbcType());
        }
    }

    /**
     * jdbc 类型转化为数据库数据类型的字符串形式
     *
     * @param jdbcType jdbc 类型
     * @return 数据类型的字符串形式
     */
    String jdbcTypeToDbTypeString(JDBCType jdbcType);

    /**
     * 构建索引
     *
     * @param indexList
     * @return
     */
    default String createIndex(List<TableIndex> indexList, List<Field> fields) {
        Map<String, Field> fieldMap = fields.stream().collect(Collectors.toMap(Field::getName, field -> field));

        StringBuilder stringBuilder = new StringBuilder();
        if (CollectionUtils.isNotEmpty(indexList)) {
            for (int i = 0; i < indexList.size(); i++) {
                TableIndex index = indexList.get(i);
                if (CollectionUtils.isNotEmpty(index.getField())) {
                    stringBuilder.append(",");
                    stringBuilder.append(indexKeyword(index.getType()));
                    stringBuilder.append(" ");
                    stringBuilder.append(decorateIdentifier("idx_" + i));
                    stringBuilder.append(" (");
                    stringBuilder.append(index.getField().stream()
                        .map(fieldName -> {
                            String decorateIdentifier = decorateIdentifier(fieldName);
                            Field field = fieldMap.get(fieldName);

                            // 字符串类型需要限制索引长度
                            if (field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Utf8
                                || field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.LargeUtf8) {
                                decorateIdentifier = decorateIdentifier + "(128)";
                            }
                            return decorateIdentifier;
                        })
                        .collect(Collectors.joining(",")));
                    stringBuilder.append(") ");
                }
            }
        }
        return stringBuilder.toString();
    }

    /**
     * 根据索引类型获取关键词
     *
     * @param indexType
     * @return
     */
    String indexKeyword(IndexType indexType);

    /**
     * 获取数据库连接
     *
     * @return
     */
    default Connection getDatabaseConn(HikariDataSource dataSource) {
        try {
            return dataSource.getConnection();
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_DATASOURCE_CONNECTION_VALIDATE_FAILED, e);
        }
    }

    /**
     * 数据序列化为字符串
     *
     * @param value 原始数据
     * @return
     */
    default String serialize(JDBCType type, Object value) {
        // 文本数据无法区分为空内容还是null，序列化为空内容
        if (value == null) {
            return null;
        }

        if (value instanceof Double || value instanceof Float || value instanceof Short || value instanceof Byte
            || value instanceof Integer || value instanceof Long || value instanceof Boolean
            || value instanceof BigDecimal || value instanceof BigInteger) {
            return value.toString();
        }

        // 字节数组单独处理
        if (value instanceof byte[]) {
            return decorateStrValue(new String((byte[]) value));
        }
        if (value instanceof Text) {
            return decorateStrValue(value.toString());
        }
        if (value instanceof LocalDateTime) {
            return decorateStrValue(((LocalDateTime) value).format(new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .toFormatter()));
        }

        // 兜底，都使用string传输
        return decorateStrValue(value.toString());
    }
}