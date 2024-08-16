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
import org.secretflow.dataproxy.common.model.datasource.conn.MysqlConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.MysqlLocationConfig;

import com.zaxxer.hikari.HikariConfig;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhong
 * @date 2023-09-07 19:56
 */
public class MysqlJdbcAssistant implements JdbcAssistant<MysqlConnConfig, MysqlLocationConfig> {
    private static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql://";
    private static final String MYSQL_CONNECTION_TEST_QUERY = "SELECT 1 FROM DUAL";
    private static final String MYSQL_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    @Override
    public String getConnectionTestQuery() {
        return MYSQL_CONNECTION_TEST_QUERY;
    }

    @Override
    public String getDriverClass() {
        return MYSQL_DRIVER_CLASS_NAME;
    }

    @Override
    public void initDataSourceConfig(HikariConfig config, MysqlConnConfig connConfig) {
        config.setJdbcUrl(MYSQL_JDBC_URL_PREFIX + connConfig.getHost());
        config.setCatalog(connConfig.getDatabase());
    }

    @Override
    public String decorateIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String composeTableName(MysqlLocationConfig locationConfig) {
        return decorateIdentifier(locationConfig.getTable());
    }

    @Override
    public String jdbcTypeToDbTypeString(JDBCType jdbcType) {
        return switch (jdbcType) {
            case TINYINT -> "TINYINT";
            case SMALLINT -> "SMALLINT";
            case INTEGER -> "INT";
            case BIGINT -> "BIGINT";
            case REAL -> "REAL";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> "DECIMAL";
            case BOOLEAN -> "BOOLEAN";
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP DEFAULT '2000-01-01 00:00:00'";
            case VARCHAR -> "TEXT";
            case LONGVARCHAR -> "LONGTEXT";
            case BINARY, VARBINARY -> "BLOB";
            case LONGVARBINARY -> "LONGBLOB";
            default -> throw DataproxyException.of(DataproxyErrorCode.UNSUPPORTED_FIELD_TYPE, jdbcType.name());
        };
    }

    @Override
    public String indexKeyword(IndexType indexType) {
        switch (indexType) {
            case UNIQUE:
                return "UNIQUE KEY";
            case INDEX:
                return "INDEX";
            default:
                throw DataproxyException.of(DataproxyErrorCode.UNSUPPORTED_INDEX_TYPE, indexType.name());
        }
    }

    @Override
    public List<String> preWorkSqls(String composeTableName, Schema schema, MysqlLocationConfig locationConfig, TableFormatConfig formatConfig) {
        List<String> preWorkSqls = new ArrayList<>();
        preWorkSqls.add(dropTableSql(composeTableName));
        String createTabelSql = "CREATE TABLE " + composeTableName + " ("
            + createTableColumnTypes(schema.getFields(), formatConfig)
            + "," + decorateIdentifier(formatConfig.getPrimaryKey())
            + " BIGINT PRIMARY KEY NOT NULL AUTO_INCREMENT"
            + createIndex(formatConfig.getIndexList(), schema.getFields())
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        preWorkSqls.add(createTabelSql);
        return preWorkSqls;
    }
}
