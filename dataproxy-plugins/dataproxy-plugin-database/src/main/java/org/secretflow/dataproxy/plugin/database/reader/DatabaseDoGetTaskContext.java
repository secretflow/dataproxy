/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;
import org.secretflow.dataproxy.core.reader.Sender;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.secretflow.dataproxy.plugin.database.utils.Record;

@Slf4j
public class DatabaseDoGetTaskContext implements AutoCloseable{

    private final Sender<Record> sender;

    private final TaskConfig taskConfig;
    private final VectorSchemaRoot root;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private final DatabaseRecordReader reader;

    private Future<?> readFuture;

    private final AtomicBoolean hasNext = new AtomicBoolean(true);

    public DatabaseDoGetTaskContext(TaskConfig taskConfig, VectorSchemaRoot root) {
        this.root = root;
        this.taskConfig = taskConfig;
        this.sender = getSender();
        this.reader = new DatabaseRecordReader(taskConfig, sender, taskConfig.getContext().getResultSet());
    }

    public void start() {
        readFuture = executorService.submit(() -> {
            try {
                reader.read();
                sender.putOver();
                hasNext.set(false);
            } catch (InterruptedException e) {
                log.error("read interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    public void cancel() {
        if (readFuture != null && !readFuture.isDone()) {
            log.info("cancel read task...");
            readFuture.cancel(true);
        }
    }
    @Override
    public void close() throws Exception {
        this.cancel();
        executorService.shutdown();
    }

    public void putNextPatchData() {
        if ((!hasNext() && readFuture.isDone()) || readFuture.isCancelled() ) {
            return;
        }
        sender.send();
    }

    public boolean hasNext() {
        return hasNext.get();
    }

    private Sender<Record> getSender() {
        int estimatedRecordCount = 1_000;
        return new DatabaseRecordSender(
                estimatedRecordCount,
                new LinkedBlockingQueue<>(estimatedRecordCount),
                root,
                taskConfig.getContext().getTableName(),
                taskConfig.getContext().getDatabaseMetaData(),
                taskConfig.getContext().getResultSet()
                );
    }
}
