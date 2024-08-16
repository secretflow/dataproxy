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
import org.secretflow.dataproxy.common.model.FlightContentFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.format.PartitionBehavior;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.manager.SplitReader;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfig;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfigBuilder;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowUtils;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author muhong
 * @date 2023-09-07 19:47
 */
@Slf4j
public class JdbcSplitReader extends ArrowReader implements SplitReader {

    private final PartitionBehavior partitionBehavior;
    /**
     * 数据库连接
     */
    private HikariDataSource dataSource;
    /**
     * 表名
     */
    private String composeTableName;
    private JdbcAssistant jdbcAssistant;
    private FlightContentFormatConfig outputFormatConfig;

    private List<String> fieldList;

    private String filter;

    private Schema schema;

    private String currentPartition = null;

    private int currentSize = 0;

    private String sqlPartTemplate;
    private JdbcToArrowConfig config;

    public JdbcSplitReader(BufferAllocator allocator,
                           JdbcAssistant jdbcAssistant,
                           FlightContentFormatConfig outputFormatConfig,
                           HikariDataSource dataSource,
                           String composeTableName,
                           Schema schema,
                           PartitionBehavior partitionBehavior,
                           List<String> fieldList,
                           String filter) {
        super(allocator);
        this.jdbcAssistant = jdbcAssistant;
        this.dataSource = dataSource;
        this.composeTableName = composeTableName;
        this.partitionBehavior = partitionBehavior;
        this.fieldList = fieldList;
        this.filter = filter;
        this.outputFormatConfig = outputFormatConfig;
        this.schema = schema;

        this.config = new JdbcToArrowConfigBuilder()
            .setAllocator(allocator)
            .setTargetBatchSize(JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE)
            // 设置浮点型精度规则，四舍五入，保证精度有损场景
            .setBigDecimalRoundingMode(RoundingMode.CEILING)
            .build();

        // 若指定读取顺序则调整schema
        if (CollectionUtils.isNotEmpty(fieldList)) {
            this.schema = new Schema(fieldList.stream().map(schema::findField).collect(Collectors.toList()));
        } else {
            this.schema = schema;
        }
    }

    private static void preAllocate(VectorSchemaRoot root, int targetSize) {
        for (ValueVector vector : root.getFieldVectors()) {
            if (vector instanceof BaseFixedWidthVector) {
                ((BaseFixedWidthVector) vector).allocateNew(targetSize);
            }
        }
    }

    @Override
    public ArrowReader startRead() {

        String fieldPart = this.jdbcAssistant.createFieldPart(this.fieldList, this.composeTableName);

        StringBuilder selectSqlBuilder = new StringBuilder().append(fieldPart);

        // sql模板替换参数列表
        if (this.partitionBehavior != null && this.partitionBehavior.isValid()) {
            log.info("[startRead] partitionBehavior: {}", JsonUtils.toJSONString(this.partitionBehavior));
            String partitionFieldName = this.jdbcAssistant.decorateIdentifier(this.partitionBehavior.getFieldName());
            selectSqlBuilder.append("where ")
                .append(partitionFieldName)
                .append(">= %s and ")
                .append(partitionFieldName)
                .append("< %s");
        }
        this.sqlPartTemplate = selectSqlBuilder.toString();
        return this;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();

        String sql = generateNextSql();
        if (StringUtils.isEmpty(sql)) {
            return false;
        }

        try (Connection conn = getDatabaseConn();
             ResultSet resultSet = conn.createStatement().executeQuery(sql)) {
            JdbcToArrowUtils.jdbcToArrowVectors(resultSet, getVectorSchemaRoot(), this.config);
        } catch (SQLException e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_FETCH_BATCH_DATA_FAILED, e);
        }
        return true;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        close();
    }

    @Override
    protected Schema readSchema() {
        return this.schema;
    }

    @Override
    public void close() {
        try {
            if (this.dataSource != null) {
                this.dataSource.close();
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    protected Connection getDatabaseConn() {
        try {
            return dataSource.getConnection();
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_GET_CONN_THREAD_FAILED, e);
        }
    }

    // 生成下一条查询sql
    private String generateNextSql() {
        String currentLowerBoundStr = null;
        String currentUpperBoundStr = null;

        // 分页规则
        if (this.partitionBehavior != null && this.partitionBehavior.isValid()) {
            String current = StringUtils.isEmpty(this.currentPartition) ? this.partitionBehavior.getLowerBound() : this.currentPartition;

            switch (this.partitionBehavior.getType()) {
                case Int: {
                    Long currentLowerBound = Long.valueOf(current);
                    if (currentLowerBound > Long.valueOf(this.partitionBehavior.getUpperBound())) {
                        return null;
                    }

                    Long currentUpperBound = currentLowerBound + (long) Math.ceil(Double.parseDouble(this.partitionBehavior.getStep()));

                    currentLowerBoundStr = String.valueOf(currentLowerBound);
                    currentUpperBoundStr = String.valueOf(currentUpperBound);
                    break;
                }
                case FloatingPoint:
                case Decimal: {
                    Double currentLowerBound = Double.valueOf(current);
                    if (currentLowerBound > Double.valueOf(this.partitionBehavior.getUpperBound())) {
                        return null;
                    }

                    Double currentUpperBound = currentLowerBound + Double.parseDouble(this.partitionBehavior.getStep());

                    currentLowerBoundStr = String.valueOf(currentLowerBound);
                    currentUpperBoundStr = String.valueOf(currentUpperBound);
                    break;
                }
                default:
                    throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "分页字段 " + this.partitionBehavior.getFieldName() + " 无法做数值计算");
            }
        } else {
            if (this.currentSize > 0) {
                return null;
            }
        }

        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put("sqlPart", String.format(this.sqlPartTemplate, currentLowerBoundStr, currentUpperBoundStr));
        valuesMap.put("limitPart", "");
        String execSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());

        this.currentPartition = currentUpperBoundStr;
        this.currentSize++;
        return execSql;
    }
}
