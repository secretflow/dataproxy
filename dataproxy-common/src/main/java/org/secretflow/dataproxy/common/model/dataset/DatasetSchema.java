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

import org.secretflow.dataproxy.common.model.dataset.schema.DatasetSchemaTypeEnum;
import org.secretflow.dataproxy.common.model.dataset.schema.FastDFSchema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * 数据集数据结构
 *
 * @author muhong
 * @date 2023-08-30 19:21
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DatasetSchema {

    /**
     * 数据集数据结构类型
     */
    private DatasetSchemaTypeEnum type;

    /**
     * 数据结构
     */
    private FastDFSchema schema;

    /**
     * arrow 类型
     */
    private Schema arrowSchema;
}
