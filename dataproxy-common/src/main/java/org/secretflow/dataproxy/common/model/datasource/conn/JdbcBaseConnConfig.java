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
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

import java.util.Map;

/**
 * JDBC datasource connection config
 *
 * @author muhong
 * @date 2023-09-07 14:06
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class JdbcBaseConnConfig implements ConnConfig {

    /**
     * Host
     */
    private String host;

    /**
     * Dataset
     */
    private String database;

    /**
     * Username
     */
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String userName;

    /**
     * Password
     */
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String password;

    /**
     * Options
     */
    private Map<String, String> option;

    @Builder.Default
    private Integer maximumPoolSize = 10;

    @Builder.Default
    private Integer minimumIdle = 2;

    @Builder.Default
    private Boolean cachePrepStmts = true;

    @Builder.Default
    private Boolean useServerPrepStmts = true;

    @Builder.Default
    private Integer prepStmtCacheSize = 200;

    @Builder.Default
    private Integer prepStmtCacheSqlLimit = 2048;

}
