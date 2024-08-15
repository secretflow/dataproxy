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

package org.secretflow.dataproxy.common.model.datasource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Datasource
 *
 * @author yumu
 * @date 2023/8/30 16:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Datasource {
    /**
     * Datasource unique id
     */
    private String datasourceId;

    /**
     * Datasource name
     */
    private String name;

    /**
     * Datasource description
     */
    private String description;

    /**
     * Datasource connection config
     */
    private DatasourceConnConfig connConfig;

    /**
     * Writable
     */
    private Boolean writable;

    /**
     * Owner id
     */
    private String ownerId;

    /**
     * Attributes
     */
    private Map<String, String> attributes;
}
