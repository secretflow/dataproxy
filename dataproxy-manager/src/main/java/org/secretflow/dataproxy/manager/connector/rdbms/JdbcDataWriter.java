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

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.dataset.format.TableFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.location.JdbcLocationConfig;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.manager.DataWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * jdbc数据源写入
 *
 * @author muhong
 * @date 2023-09-08 15:37
 */
@Slf4j
public class JdbcDataWriter implements DataWriter {

    /**
     * 数据库连接
     */
    protected HikariDataSource dataSource;

    /**
     * 表名
     */
    protected String composeTableName;

    protected JdbcAssistant jdbcAssistant;

    protected JdbcLocationConfig locationConfig;

    protected TableFormatConfig formatConfig;

    //the statement in the format of either merge into or insert into sql statement
    protected String stmt;

    private boolean initialized;

    public JdbcDataWriter() {
    }

    public JdbcDataWriter(JdbcAssistant jdbcAssistant, HikariDataSource dataSource, String composeTableName, JdbcLocationConfig locationConfig, TableFormatConfig formatConfig, Schema schema) {
        this.jdbcAssistant = jdbcAssistant;
        this.dataSource = dataSource;
        this.initialized = false;
        this.formatConfig = formatConfig;
        this.locationConfig = locationConfig;

        this.composeTableName = composeTableName;

        ensureInitialized(schema);
    }

    protected void ensureInitialized(Schema schema) {
        if (!this.initialized) {
            this.initialize(schema);
            this.initialized = true;
        }
    }

    protected void initialize(Schema schema) {
        List<String> preSqlList = this.jdbcAssistant.preWorkSqls(this.composeTableName, schema, this.locationConfig, this.formatConfig);
        log.info("[JdbcDataWriter] preSql execute start, sql: {}", JsonUtils.toJSONString(preSqlList));

        try (Connection conn = this.jdbcAssistant.getDatabaseConn(dataSource)) {
            // do nothing
            // Avoid SQL injection issues
            // About to Delete
        } catch (SQLException e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_CREATE_TABLE_FAILED, e.getMessage(), e);
        }

        // 构造sql预提交模板
        this.stmt = String.format("insert into %s(%s) values(%s)", composeTableName,
            String.join(",", schema.getFields().stream().map(field -> this.jdbcAssistant.decorateIdentifier(field.getName())).toArray(String[]::new)),
            String.join(",", schema.getFields().stream().map(field -> "?").toArray(String[]::new)));
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        throw DataproxyException.of(DataproxyErrorCode.JDBC_INSERT_INTO_TABLE_FAILED, "jdbc not support write");
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void destroy() throws IOException {

    }

    @Override
    public void close() throws Exception {
        try {
            if (this.dataSource != null) {
                this.dataSource.close();
            }
        } catch (Exception ignored) {
        }
    }
}