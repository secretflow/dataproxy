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

package org.secretflow.dataproxy.common.model.datasource.conn;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

/**
 * connection configuration of odps
 *
 * @author yuexie
 * @date 2024-05-30 10:30:20
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OdpsConnConfig implements ConnConfig {

    /**
     * access key id
     */
    @NotBlank
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessKeyId;

    /**
     * access key secret
     */
    @NotBlank
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessKeySecret;

    /**
     * endpoint
     */
    @NotBlank
    private String endpoint;

    /**
     * project name
     */
    private String projectName;

}
