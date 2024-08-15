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

package org.secretflow.dataproxy.common.model.dataset.format;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Table format config
 * @author muhong
 * @date 2023-08-30 19:36
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class TableFormatConfig implements FormatConfig {
    /**
     * Primary key
     */
    private String primaryKey;

    /**
     * Index list
     */
    private List<TableIndex> indexList;

    /**
     * Partition behavior
     */
    private PartitionBehavior partitionBehavior;

    /**
     * Field name map
     */
    private Map<String, String> fieldMap;
}
