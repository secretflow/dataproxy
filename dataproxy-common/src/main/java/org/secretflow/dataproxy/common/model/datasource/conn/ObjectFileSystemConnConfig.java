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

package org.secretflow.dataproxy.common.model.datasource.conn;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

/**
 * Oss datasource connection config
 *
 * @author muhong
 * @date 2023-09-11 11:34
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class ObjectFileSystemConnConfig implements ConnConfig {

    /**
     * 地址
     */
    private String endpoint;

    /**
     * 访问秘钥
     */
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessKey;

    /**
     * ak的密码
     */
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessSecret;

    /**
     * 通信协议，http 或 https
     */
    private String endpointProtocol;

    /**
     * 区域域名，不带 host 和 protocol
     */
    private String regionHost;

    /**
     * bucket
     */
    private String bucket;

    /**
     * 对象 key 前缀，为 prefix/ 形式
     */
    private String objectKeyPrefix;
}
