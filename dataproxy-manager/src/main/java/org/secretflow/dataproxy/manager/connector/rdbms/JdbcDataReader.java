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
import org.secretflow.dataproxy.common.model.dataset.format.TableFormatConfig;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.SplitReader;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfig;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowConfigBuilder;
import org.secretflow.dataproxy.manager.connector.rdbms.adaptor.JdbcToArrowUtils;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author muhong
 * @date 2023-09-07 17:57
 */
public class JdbcDataReader implements DataReader {

    protected static final int PARTITION_STAT_SAMPLE_ROW_COUNT = 100;
    private static final long MEMORY_LIMIT = 100 * 1024 * 1024;
    /**
     * 数据库连接
     */
    private final HikariDataSource dataSource;
    private final JdbcAssistant jdbcAssistant;
    private final BufferAllocator allocator;
    private final TableFormatConfig sourceSchemaConfig;
    private final FlightContentFormatConfig outputFormatConfig;
    private final List<String> fieldList;
    private final String filter;
    /**
     * 数据源连接配置
     */
    private final String composeTableName;
    private final Schema schema;


    public JdbcDataReader(BufferAllocator allocator,
                          JdbcAssistant jdbcAssistant,
                          HikariDataSource dataSource,
                          TableFormatConfig sourceSchemaConfig,
                          FlightContentFormatConfig outputFormatConfig,
                          String composeTableName,
                          Schema schema,
                          List<String> fieldList,
                          String filter) {

        this.allocator = allocator;
        this.jdbcAssistant = jdbcAssistant;
        this.dataSource = dataSource;
        this.schema = schema;
        this.sourceSchemaConfig = sourceSchemaConfig;
        this.fieldList = fieldList;
        this.filter = filter;
        this.outputFormatConfig = outputFormatConfig;
        this.composeTableName = composeTableName;
    }

    @Override
    public List<SplitReader> createSplitReader(int splitNumber) {
        fillPartitionBehavior();

        // todo: 根据splitNumber与partitionBehavior分区
        return Arrays.asList(new JdbcSplitReader(allocator, jdbcAssistant, outputFormatConfig, dataSource, composeTableName, schema, this.sourceSchemaConfig.getPartitionBehavior(), fieldList, filter));
    }

    private void fillPartitionBehavior() {
        try {
            PartitionBehavior partitionBehavior = sourceSchemaConfig.getPartitionBehavior();
            if (partitionBehavior == null || StringUtils.isEmpty(partitionBehavior.getFieldName())) {
                return;
            }

            String partitionField = this.jdbcAssistant.decorateIdentifier(partitionBehavior.getFieldName());

            try (Connection conn = this.jdbcAssistant.getDatabaseConn(dataSource)) {

                // 设置最大值
                if (StringUtils.isEmpty(partitionBehavior.getUpperBound())) {
                    Map<String, String> valuesMap = new HashMap<>();
                    valuesMap.put("sqlPart", "max(" + partitionField + ") from " + composeTableName);
                    valuesMap.put("limitPart", "");
                    String maxPkSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());
                    try (ResultSet maxPkRs = conn.createStatement().executeQuery(maxPkSql)) {
                        if (!maxPkRs.next()) {
                            return;
                        }
                        String maxStr = maxPkRs.getObject(1).toString();
                        partitionBehavior.setUpperBound(maxStr);
                    }
                }

                // 设置最小值
                if (StringUtils.isEmpty(partitionBehavior.getLowerBound())) {
                    Map<String, String> valuesMap = new HashMap<>();
                    valuesMap.put("sqlPart", "min(" + partitionField + ") from " + composeTableName);
                    valuesMap.put("limitPart", "");
                    String minPkSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());
                    try (ResultSet minPkRs = conn.createStatement().executeQuery(minPkSql)) {
                        if (!minPkRs.next()) {
                            return;
                        }
                        String minStr = minPkRs.getObject(1).toString();
                        partitionBehavior.setLowerBound(minStr);
                    }
                }

                // 步进间隔估计
                if (StringUtils.isEmpty(partitionBehavior.getStep())) {
                    Map<String, String> valuesMap = new HashMap<>();

                    // 计算总数
                    long count = 0;
                    valuesMap.put("sqlPart", "count(*) from " + composeTableName);
                    valuesMap.put("limitPart", "");
                    String countSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());
                    try (ResultSet countRs = conn.createStatement().executeQuery(countSql)) {
                        countRs.next();
                        count = countRs.getLong(1);
                    }
                    // 数据集中没有数据，则直接返回
                    if (count == 0) {
                        return;
                    }

                    valuesMap.put("sqlPart", this.jdbcAssistant.createFieldPart(this.fieldList, this.composeTableName));
                    valuesMap.put("limitPart", String.format(
                        this.jdbcAssistant.generateLimitConditionTemplate(false), PARTITION_STAT_SAMPLE_ROW_COUNT
                    ));
                    String sampleSql = new StringSubstitutor(valuesMap).replace(this.jdbcAssistant.selectSQLTemplate());
                    try (ResultSet sampleRs = conn.createStatement().executeQuery(sampleSql)) {
                        // 将示例数据转换为arrow
                        int sampleRowCount = 0;
                        long sampleDataSize = 0;

                        // 设置浮点型精度规则，四舍五入，保证精度有损场景
                        JdbcToArrowConfig config = new JdbcToArrowConfigBuilder()
                            .setAllocator(allocator)
                            .setTargetBatchSize(PARTITION_STAT_SAMPLE_ROW_COUNT)
                            .setBigDecimalRoundingMode(RoundingMode.CEILING)
                            .build();

                        try (VectorSchemaRoot root = VectorSchemaRoot.create(
                            JdbcToArrowUtils.jdbcToArrowSchema(sampleRs.getMetaData(), config), config.getAllocator())) {
                            if (config.getTargetBatchSize() != JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
                                ValueVectorUtility.preAllocate(root, config.getTargetBatchSize());
                            }
                            JdbcToArrowUtils.jdbcToArrowVectors(sampleRs, root, config);
                            // 总空间大小为每列求和
                            sampleRowCount = root.getRowCount();
                            sampleDataSize = root.getFieldVectors().stream().map(ValueVector::getBufferSize).reduce(0, Integer::sum);
                        }

                        // 步进间隔 = (最大值 - 最小值) / ((采样大小 / 采样行数) * 总行数 / 期望内存 + 1)
                        BigDecimal dataInterval = new BigDecimal(partitionBehavior.getUpperBound()).divide(new BigDecimal(partitionBehavior.getLowerBound()));
                        if (dataInterval.compareTo(BigDecimal.ZERO) == 0) {
                            partitionBehavior.setStep("1");
                        } else {
                            int partitionSize = (int) Math.ceil((double) sampleDataSize / sampleRowCount * count / MEMORY_LIMIT);
                            BigDecimal step = dataInterval.divide(new BigDecimal(partitionSize), MathContext.DECIMAL32);
                            partitionBehavior.setStep(step.toString());
                        }
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.JDBC_GET_PARTITION_STATS_FAILED, e);
        }
    }
}
