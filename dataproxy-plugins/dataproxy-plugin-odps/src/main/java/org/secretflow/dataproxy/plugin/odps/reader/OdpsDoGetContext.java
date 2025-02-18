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

package org.secretflow.dataproxy.plugin.odps.reader;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.plugin.odps.config.OdpsCommandConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableQueryConfig;
import org.secretflow.dataproxy.plugin.odps.config.ScqlCommandJobConfig;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;
import org.secretflow.dataproxy.plugin.odps.utils.OdpsUtil;
import org.secretflow.v1alpha1.common.Common;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * @author yuexie
 * @date 2024/11/26 13:48
 **/
@Slf4j
public class OdpsDoGetContext {

    private final OdpsCommandConfig<?> odpsCommandConfig;

    @Getter
    private InstanceTunnel.DownloadSession downloadSession;

    @Getter
    private long count;

    @Getter
    private Schema schema;

    private final boolean isLazyMode;

    private Future<?> doSqlTaskFuture = null;

    private final CountDownLatch sqlTaskRunCountDownLatch = new CountDownLatch(1);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Map<byte[], ParamWrapper> ticketWrapperMap = new ConcurrentHashMap<>();
    private final ReadWriteLock tickerWrapperMapRwLock = new ReentrantReadWriteLock();

    public OdpsDoGetContext(OdpsCommandConfig<?> config) {
        this(config, true);
    }

    public OdpsDoGetContext(OdpsCommandConfig<?> config, boolean isLazyMode) {
        this.odpsCommandConfig = config;
        this.isLazyMode = isLazyMode;
        prepare();
    }

    private void prepare() {

        // 2. init download session
        OdpsConnectConfig odpsConnectConfig = odpsCommandConfig.getOdpsConnectConfig();
        Odps odps = OdpsUtil.initOdps(odpsConnectConfig);
        String querySql;

        if (odpsCommandConfig instanceof ScqlCommandJobConfig scqlReadJobConfig) {
            querySql = scqlReadJobConfig.getCommandConfig();
        } else if (odpsCommandConfig instanceof OdpsTableQueryConfig odpsTableQueryConfig) {
            OdpsTableConfig tableConfig = odpsTableQueryConfig.getCommandConfig();
            // If the DomainData is parsed to SQL,
            // the query is performed by the field of the DomainData and the field value of the column of the DomainData is returned
            querySql = this.buildSql(odps, tableConfig.tableName(), tableConfig.columns().stream().map(Common.DataColumn::getName).toList(), tableConfig.partition());
            this.schema = odpsCommandConfig.getResultSchema();

        } else {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Unsupported read parameter type: " + odpsCommandConfig.getClass());
        }

        if (isLazyMode) {
            this.count = -1;
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            doSqlTaskFuture = executorService.submit(() -> this.executeSqlTaskAndHandleResult(odps, odpsConnectConfig.projectName(), querySql));
        } else {
            this.executeSqlTaskAndHandleResult(odps, odpsConnectConfig.projectName(), querySql);
        }
    }

    public List<TaskConfig> getTaskConfigs() {
        return Collections.singletonList(new TaskConfig(this, 0, count));
    }

    /**
     * Execute SQL task and handle the result.
     *
     * @param odps        the Odps instance
     * @param projectName the project name
     * @param querySql    the SQL query to execute
     */
    private void executeSqlTaskAndHandleResult(Odps odps, String projectName, String querySql) {
        Throwable throwable = null;
        try {
            readWriteLock.writeLock().lock();
            sqlTaskRunCountDownLatch.countDown();

            // 1. run SQL task
            Instance runInstance = SQLTask.run(odps, projectName, querySql, OdpsUtil.getSqlFlag(), null);
            runInstance.waitForSuccess();
            log.debug("SQL Task run success, sql: {}", querySql);

            // 2. handle result
            if (runInstance.isSuccessful()) {
                downloadSession = new InstanceTunnel(odps).createDownloadSession(projectName, runInstance.getId(), false);
                this.count = downloadSession.getRecordCount();
            } else {
                log.error("SQL Task run result is not successful, sql: {}", querySql);
                throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "SQL Task run result is not successful, sql: " + querySql);
            }

            // 3. init schema
            // If the SQL is directly queried, the decision returned by SQL in the returned arrow schema is initialized here
            // If it is determined by table, the field defined by DomainData is returned and initialized in a different way
            if (odpsCommandConfig.getOdpsTypeEnum() == OdpsTypeEnum.SQL) {
                this.initArrowSchemaFromColumns();
            }
        } catch (OdpsException e) {
            log.error("SQL Task run error", e);
            throwable = e;
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, e.getMessage(), e);
        } catch (Exception e) {
            throwable = e;
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "SQL Task run error", e);
        }finally {
            readWriteLock.writeLock().unlock();
            loadLazyConfig(throwable);
        }
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

    private void initArrowSchemaFromColumns() {
        schema = new Schema(downloadSession.getSchema().getAllColumns().stream().map(OdpsUtil::convertOdpsColumnToArrowField).toList());
    }

    private String buildSql(Odps odps, String tableName, List<String> fields, String whereClause) {

        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        boolean partitioned = odps.tables().get(tableName).isPartitioned();
        // Common tables no longer concatenate conditional statements
        if (!partitioned) {
            whereClause = "";
        }

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

        log.debug("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause) + ";";
    }

}
