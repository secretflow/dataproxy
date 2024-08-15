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

import org.secretflow.dataproxy.common.model.datasource.location.LocationConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Dataset location config
 *
 * @author muhong
 * @date 2023-08-30 19:18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DatasetLocationConfig {

    /**
     * Datasource id
     */
    private String datasourceId;

    /**
     * Datasource type
     */
    private DatasourceTypeEnum type;

    /**
     * Location in its datasource
     */
    private LocationConfig locationConfig;
}
