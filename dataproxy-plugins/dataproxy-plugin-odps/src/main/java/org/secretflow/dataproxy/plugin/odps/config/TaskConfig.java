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

package org.secretflow.dataproxy.plugin.odps.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.secretflow.dataproxy.plugin.odps.reader.OdpsDoGetContext;

/**
 * @author yuexie
 * @date 2024/10/30 17:46
 **/
@Getter
@ToString
public class TaskConfig {

    @JsonIgnore
    private final OdpsDoGetContext context;

    /**
     * Start index
     */
    private final long startIndex;

    /**
     * Count
     */
    private final long count;

    /**
     * Current index: Reserved field, endpoint resume
     */
    @Setter
    private long currentIndex;

    /**
     * Whether to compress
     */
    private final boolean compress;

    @Getter
    @Setter
    private Throwable error;

    public TaskConfig(OdpsDoGetContext context, long startIndex, long count) {
        this(context, startIndex, count, true);
    }

    public TaskConfig(OdpsDoGetContext context, long startIndex, long count, boolean compress) {
        this.context = context;
        this.startIndex = startIndex;
        this.count = count;
        this.compress = compress;
        this.currentIndex = startIndex;
    }
}
