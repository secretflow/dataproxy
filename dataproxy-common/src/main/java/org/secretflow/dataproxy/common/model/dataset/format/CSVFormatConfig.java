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
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * CSV format config
 *
 * @author muhong
 * @date 2023-08-30 19:32
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CSVFormatConfig implements FormatConfig {

    /**
     * Field name map, key: raw name, value:output name
     */
    Map<String, String> fieldMap;

    /**
     * With header line
     */
    @Builder.Default
    private Boolean withHeaderLine = true;

    /**
     * Separator
     */
    @Builder.Default
    private String separator = ",";

    /**
     * QuoteChar
     */
    @Builder.Default
    private String quoteChar = "\"";

    /**
     * EscapeChar
     */
    @Builder.Default
    private String escapeChar = "\\";
}
