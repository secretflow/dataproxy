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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Partition behavior
 *
 * @author muhong
 * @date 2023-10-23 15:44
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PartitionBehavior {

    /**
     * Field name
     */
    private String fieldName;

    /**
     * Field type
     */
    private ArrowType.ArrowTypeID type;

    /**
     * Lower bound
     */
    private String lowerBound;

    /**
     * Upper bound
     */
    private String upperBound;

    /**
     * Partition step
     */
    private String step;

    /**
     * Predicates, eg["id>=0 AND id<100", "id>=100 AND id<200", "id>=200 AND id<300"]
     */
    private List<String> predicates;

    public boolean isValid() {
        return StringUtils.isNotEmpty(fieldName) && type != null && StringUtils.isNotEmpty(lowerBound) && StringUtils.isNotEmpty(upperBound);
    }
}
