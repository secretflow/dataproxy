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

import org.secretflow.dataproxy.common.model.datasource.DatasetLocationConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Dataset
 *
 * @author muhong
 * @date 2023-08-30 19:20
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Dataset {

    /**
     * Dataset unique id
     */
    private String datasetId;

    /**
     * Dataset name
     */
    private String name;

    /**
     * Dataset description
     */
    private String description;

    /**
     * Dataset scene
     */
    private DataSceneEnum dataScene;

    /**
     * Dataset location in its datasource
     */
    private DatasetLocationConfig locationConfig;

    /**
     * Dataset format config
     */
    private DatasetFormatConfig formatConfig;

    /**
     * Dataset schema
     */
    private DatasetSchema schema;

    /**
     * Dataset owner id
     */
    private String ownerId;

    /**
     * Attributes
     */
    private Map<String, String> attributes;
}
