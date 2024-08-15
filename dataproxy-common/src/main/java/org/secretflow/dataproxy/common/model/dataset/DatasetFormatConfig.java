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

package org.secretflow.dataproxy.common.model.dataset;

import org.secretflow.dataproxy.common.model.dataset.format.DatasetFormatTypeEnum;
import org.secretflow.dataproxy.common.model.dataset.format.FormatConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Dataset format config
 *
 * @author muhong
 * @date 2023-08-30 19:20
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DatasetFormatConfig {

    /**
     * Format type
     */
    private DatasetFormatTypeEnum type;

    /**
     * Format content
     */
    private FormatConfig formatConfig;
}
