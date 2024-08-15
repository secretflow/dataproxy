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

package org.secretflow.dataproxy.manager.connector.filesystem;

import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.SplitReader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

/**
 * Binary file data reader
 *
 * @author yumu
 * @date 2023/9/12 19:17
 */
public class BinaryFileDataReader implements DataReader {

    private final BufferAllocator allocator;

    private final FileSystem fileSystem;

    private final String uri;

    public BinaryFileDataReader(BufferAllocator allocator,
                                FileSystem fileSystem,
                                String uri) {
        this.allocator = allocator;
        this.fileSystem = fileSystem;
        this.uri = uri;
    }

    @Override
    public List<SplitReader> createSplitReader(int splitNumber) {
        return Arrays.asList(new BinaryFileSplitReader(allocator, fileSystem, uri));
    }
}
