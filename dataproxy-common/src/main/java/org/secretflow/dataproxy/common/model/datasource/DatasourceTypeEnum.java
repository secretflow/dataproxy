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

import lombok.Getter;

/**
 * Datasource type enum
 *
 * @author yumu
 * @date 2023/8/29 14:39
 */
@Getter
public enum DatasourceTypeEnum {

    /**
     * Local filesystem
     */
    LOCAL_HOST("file:///"),

    MYSQL,

    OSS("oss://"),
    MINIO("s3a://"),
    OBS("obs://"),

    /**
     * max compute
     */
    ODPS,
    ;

    private String scheme;

    DatasourceTypeEnum() {
    }

    DatasourceTypeEnum(String scheme) {
        this.scheme = scheme;
    }
}
