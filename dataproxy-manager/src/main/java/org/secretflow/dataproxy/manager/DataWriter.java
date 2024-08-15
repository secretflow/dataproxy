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

package org.secretflow.dataproxy.manager;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/**
 * Dataset writer
 *
 * @author muhong
 * @date 2023-08-21 17:54
 */
public interface DataWriter extends AutoCloseable {

    /**
     * Write a batch
     *
     * @param root Batch to write
     */
    void write(VectorSchemaRoot root) throws IOException;

    /**
     * Write the remaining data in the buffer
     */
    void flush() throws IOException;

    /**
     * Destroy the data
     */
    void destroy() throws IOException;
}
