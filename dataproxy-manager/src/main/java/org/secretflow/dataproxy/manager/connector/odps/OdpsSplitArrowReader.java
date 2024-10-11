/*
 * Copyright 2024 Ant Group Co., Ltd.
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
package org.secretflow.dataproxy.manager.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.manager.SplitReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * odps Table Split Reader
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
@Slf4j
public class OdpsSplitArrowReader extends ArrowReader implements SplitReader, AutoCloseable {

    private final OdpsConnConfig odpsConnConfig;

    private final OdpsTableInfo tableInfo;

    private final Schema schema;

    private final int batchSize = 10000;

    private boolean partitioned = false;

    private InstanceTunnel.DownloadSession downloadSession;

    private int currentIndex = 0;

    private final Set<String> columns = new HashSet<>();

    private final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<>(batchSize);

    protected OdpsSplitArrowReader(BufferAllocator allocator, OdpsConnConfig odpsConnConfig, OdpsTableInfo tableInfo, Schema schema) {
        super(allocator);
        this.odpsConnConfig = odpsConnConfig;
        this.tableInfo = tableInfo;
        this.schema = schema;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();
        long resultCount = downloadSession.getRecordCount();
        log.info("Load next batch start, recordCount: {}", resultCount);

        if (currentIndex >= resultCount) {
            return false;
        }

        int recordCount = 0;

        try (TunnelRecordReader records = downloadSession.openRecordReader(currentIndex, batchSize, true)) {

            Record firstRecord = records.read();
            if (firstRecord != null) {
                ValueVectorUtility.preAllocate(root, batchSize);
                root.setRowCount(batchSize);

                root.getFieldVectors().forEach(fieldVector -> {
                    if (fieldVector instanceof FixedWidthVector baseFixedWidthVector) {
                        baseFixedWidthVector.allocateNew(batchSize);
                    } else if (fieldVector instanceof VariableWidthVector baseVariableWidthVector){
                        baseVariableWidthVector.allocateNew(batchSize * 32);
                    }
                });


                Future<?> submitFuture = executorService.submit(() -> {
                    try {
                        int takeRecordCount = 0;

                        for(;;) {

                            Record record = recordQueue.take();

                            if (record instanceof ArrayRecord && record.getColumns().length == 0) {
                                log.info("recordQueue take record take Count: {}", takeRecordCount);
                                break;
                            }

                            ValueVectorUtility.ensureCapacity(root, takeRecordCount + 1);
                            this.toArrowVector(record, root, takeRecordCount);
                            takeRecordCount++;

                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

                columns.addAll(Arrays.stream(firstRecord.getColumns()).map(Column::getName).collect(Collectors.toSet()));

                recordQueue.put(firstRecord);
                recordCount++;
                // 使用 #read() 方法迭代读取，将会处理历史的 record 记录的数据，异步时，将读取不到数据，可使用 #clone() 方法，性能差距不大
                for (Record record : records) {
                    try {
                        recordQueue.put(record);
                        recordCount++;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                recordQueue.put(new ArrayRecord(new Column[0]));
                log.info("recordQueue put record Count: {}", recordCount);

                submitFuture.get();
                currentIndex += batchSize;
            } else {
                log.warn("Read first record is null, maybe it has been read.");
            }

        } catch (TunnelException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        root.setRowCount(recordCount);
        log.info("Load next batch success, recordCount: {}", recordCount);
        return true;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        executorService.shutdownNow();
    }

    @Override
    protected Schema readSchema() throws IOException {
        return this.schema;
    }

    @Override
    public ArrowReader startRead() {

        Odps odps = OdpsUtil.buildOdps(odpsConnConfig);
        String sql = "";
        try {
            partitioned = odps.tables().get(odpsConnConfig.getProjectName(), tableInfo.tableName()).isPartitioned();

            sql = this.buildSql(tableInfo.tableName(), tableInfo.fields(), tableInfo.partitionSpec());
            Instance instance = SQLTask.run(odps, odpsConnConfig.getProjectName(), sql, OdpsUtil.getSqlFlag(), null);

            log.info("SQLTask run start, sql: {}", sql);
            // 等待任务完成
            instance.waitForSuccess();
            log.info("SQLTask run success, sql: {}", sql);

            downloadSession = new InstanceTunnel(odps).createDownloadSession(odps.getDefaultProject(), instance.getId(), false);

        } catch (OdpsException e) {
            log.error("SQLTask run error, sql: {}", sql, e);
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, e.getMessage(), e);
        }

        return this;
    }

    private String buildSql(String tableName, List<String> fields, String whereClause) {

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        // 普通表不再拼接条件语句
        if (!partitioned) {
            whereClause = "";
        }
        //TODO: 条件判断逻辑调整
        if (!whereClause.isEmpty()) {
            String[] groups = whereClause.split("[,/]");
            if (groups.length > 1) {
                final PartitionSpec partitionSpec = new PartitionSpec(whereClause);

                for (String key : partitionSpec.keys()) {
                    if (!columnOrValuePattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }
                    if (!columnOrValuePattern.matcher(partitionSpec.get(key)).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + partitionSpec.get(key));
                    }
                }

                List<String> list = partitionSpec.keys().stream().map(k -> k + "='" + partitionSpec.get(k) + "'").toList();
                whereClause = String.join(" and ", list);
            }
        }

        log.info("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause) + ";";
    }

    private void toArrowVector(Record record, VectorSchemaRoot root, int rowIndex) {
        FieldVector vector;
        String columnName;
        for (Field field : schema.getFields()) {
            vector = root.getVector(field);
            if (vector != null) {
                // odps 获取到的字段名为小写，此处做一下兼容
                columnName = field.getName().toLowerCase();

                if (this.hasColumn(columnName)) {
                    this.setValue(vector.getField().getType(), vector, rowIndex, record, columnName);
                }
            }
        }
    }

    private boolean hasColumn(String columnName) {
        return columns.contains(columnName);
    }

    private void setValue(ArrowType type, FieldVector vector, int rowIndex, Record record, String columnName) {
        Object columnValue = record.get(columnName);
        log.debug("columnName: {} type ID: {}, index:{}, value: {}", columnName, type.getTypeID(), rowIndex, columnValue);

        if (columnValue == null) {
            vector.setNull(rowIndex);
//            log.warn("set null, columnName: {} type ID: {}, index:{}, value: {}", columnName, type.getTypeID(), rowIndex, record);
            return;
        }
        switch (type.getTypeID()) {
            case Int -> {
                if (vector instanceof SmallIntVector smallIntVector) {
                    smallIntVector.set(rowIndex, Short.parseShort(columnValue.toString()));
                } else if (vector instanceof IntVector intVector) {
                    intVector.set(rowIndex, Integer.parseInt(columnValue.toString()));
                } else if (vector instanceof BigIntVector bigIntVector) {
                    bigIntVector.set(rowIndex, Long.parseLong(columnValue.toString()));
                } else if (vector instanceof TinyIntVector tinyIntVector) {
                    tinyIntVector.set(rowIndex, Byte.parseByte(columnValue.toString()));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }
            case Utf8 -> {
                if (vector instanceof VarCharVector varcharVector) {
                    // record#getBytes default is UTF-8
                    varcharVector.setSafe(rowIndex, record.getBytes(columnName));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }
            case FloatingPoint -> {
                if (vector instanceof Float4Vector floatVector) {
                    floatVector.set(rowIndex, Float.parseFloat(columnValue.toString()));
                } else if (vector instanceof Float8Vector doubleVector) {
                    doubleVector.set(rowIndex, Double.parseDouble(columnValue.toString()));
                } else {
                    log.warn("Unsupported type: {}", type);
                }
            }
            case Bool -> {
                if (vector instanceof BitVector bitVector) {

                    // 	switch str {
                    //	case "1", "t", "T", "true", "TRUE", "True":
                    //		return true, nil
                    //	case "0", "f", "F", "false", "FALSE", "False":
                    //		return false, nil
                    bitVector.set(rowIndex, record.getBoolean(columnName) ? 1 : 0);
                } else {
                    log.warn("ArrowType ID is Bool: Unsupported type: {}", vector.getClass());
                }
            }

            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        }

    }
}
