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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.core.reader.AbstractReader;
import org.secretflow.dataproxy.core.reader.Sender;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;

import java.time.Instant;

/**
 * @author yuexie
 * @date 2024/10/31 20:16
 **/
@Slf4j
public class OdpsTunnelRecordReader extends AbstractReader<TaskConfig, Record> {

    private final InstanceTunnel.DownloadSession downloadSession;

    public OdpsTunnelRecordReader(TaskConfig param, Sender<Record> sender, InstanceTunnel.DownloadSession downloadSession) {
        super(param, sender);

        this.downloadSession = downloadSession;
    }

    @Override
    protected void read(TaskConfig param) {
        log.info("Start read odps tunnel record reader. download session: {} start: {}, count: {}",
                downloadSession.getId(),
                param.getStartIndex(),
                param.getCount());
        try (TunnelRecordReader records =
                     downloadSession.openRecordReader(param.getStartIndex(), param.getCount(), param.isCompress())) {

            int recordCount = 0;
            Instant startInstant = Instant.now();
            Instant tempInstant;
            for (Record record : records) {
                this.put(record);
                recordCount++;

                // Every 10,000 entries is counted whether the task has been returned, and if it has been returned, the read will be interrupted
                if (recordCount % 10_000 == 0) {
                    if (Thread.currentThread().isInterrupted()) {
                        log.info("OdpsTunnelRecordReader read interrupted.  download sessionID: {} recordCount: {}", downloadSession.getId(), recordCount);
                        throw new InterruptedException("OdpsTunnelRecordReader read interrupted.");
                    }
                    tempInstant = Instant.now();
                    // 10s print the progress once
                    if (tempInstant.getEpochSecond() - startInstant.getEpochSecond() > 10) {
                        log.info("OdpsTunnelRecordReader read: download sessionID: {} recordCount: {}", downloadSession.getId(), recordCount);
                        startInstant = tempInstant;
                    }

                }
            }
            log.info("Read odps tunnel record reader finish. download sessionID: {}, start: {}, count: {}, actual number of records: {}",
                    downloadSession.getId(),
                    param.getStartIndex(),
                    param.getCount(),
                    recordCount);
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.ODPS_ERROR, "ODPS read error", e);
        }

    }
}
