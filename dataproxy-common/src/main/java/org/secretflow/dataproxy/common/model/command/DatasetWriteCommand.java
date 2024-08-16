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

package org.secretflow.dataproxy.common.model.command;

import org.secretflow.dataproxy.common.model.FlightContentFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasetLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasourceConnConfig;

import lombok.*;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * 数据存储指令
 *
 * @author muhong
 * @date 2023-08-31 11:31
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DatasetWriteCommand implements CommandInfo {

    /**
     * Datasource connection config
     */
    private DatasourceConnConfig connConfig;

    /**
     * The location of the dataset in its datasource
     */
    private DatasetLocationConfig locationConfig;

    /**
     * Data format config
     */
    private DatasetFormatConfig formatConfig;

    /**
     * Data arrow schema
     */
    private Schema schema;

    /**
     * Data input format config
     */
    private FlightContentFormatConfig inputFormatConfig;

    /**
     * extra options
     */
    private Map<String, String> extraOptions;
}
