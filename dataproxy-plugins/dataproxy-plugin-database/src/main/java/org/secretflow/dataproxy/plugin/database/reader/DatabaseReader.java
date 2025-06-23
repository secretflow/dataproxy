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

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;

import java.io.IOException;
import java.util.Optional;

@Setter
@Slf4j
public class DatabaseReader extends ArrowReader {
    private DatabaseDoGetTaskContext dbDoGetTaskContext = null;
    private final TaskConfig taskConfig;

    public DatabaseReader(BufferAllocator allocator, TaskConfig taskConfig) {
        super(allocator);
        this.taskConfig = taskConfig;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        Optional.ofNullable(taskConfig.getError()).ifPresent(e -> {
            log.error("TaskConfig is happened error: {}", e.getMessage());
            throw new RuntimeException(e);
        });

        if(dbDoGetTaskContext == null) {
            prepare();
        }

        if(dbDoGetTaskContext.hasNext()) {
            dbDoGetTaskContext.putNextPatchData();
            return true;
        }
        return false;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource(){
        try {
            if (dbDoGetTaskContext != null) {
                dbDoGetTaskContext.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Schema readSchema() {
        return taskConfig.getContext().getSchema();
    }

    public void prepare() throws IOException {
        dbDoGetTaskContext = new DatabaseDoGetTaskContext(this.taskConfig, this.getVectorSchemaRoot());
        dbDoGetTaskContext.start();
    }
}
