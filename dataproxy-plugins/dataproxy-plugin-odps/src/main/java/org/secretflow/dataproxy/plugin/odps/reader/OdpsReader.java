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

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;

import java.io.IOException;
import java.util.Optional;

/**
 * @author yuexie
 * @date 2024/10/30 17:56
 **/
@Setter
@Slf4j
public class OdpsReader extends ArrowReader {

    private OdpsDoGetTaskContext odpsDoGetTaskContext = null;

    private final TaskConfig taskConfig;

    public OdpsReader(BufferAllocator allocator, TaskConfig taskConfig) {
        super(allocator);

        this.taskConfig = taskConfig;
    }

    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    @Override
    public boolean loadNextBatch() throws IOException {
        Optional.ofNullable(taskConfig.getError()).ifPresent(e -> {
            log.error("TaskConfig is happened error: {}", e.getMessage());
            throw new RuntimeException(e);
        });

        if (taskConfig.getCount() == 0) {
            log.warn("TaskConfig count is 0, skip read data");
            return false;
        }

        if (odpsDoGetTaskContext == null) {
            prepare();
        }

        if (odpsDoGetTaskContext.hasNext()) {
            odpsDoGetTaskContext.putNextPatchData();
            return true;
        }
        return false;
    }

    private void prepare() throws IOException {
        odpsDoGetTaskContext = new OdpsDoGetTaskContext(taskConfig, this.getVectorSchemaRoot());
        odpsDoGetTaskContext.start();
    }

    /**
     * Return the number of bytes read from the ReadChannel.
     *
     * @return number of bytes read
     */
    @Override
    public long bytesRead() {
        return 0;
    }

    /**
     * Close the underlying read source.
     *
     */
    @Override
    protected void closeReadSource() {
        try {
            if (odpsDoGetTaskContext != null) {
                odpsDoGetTaskContext.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the Schema from the source, will be invoked at the beginning the initialization.
     *
     * @return the read Schema
     */
    @Override
    protected Schema readSchema() {
        return taskConfig.getContext().getSchema();
    }
}
