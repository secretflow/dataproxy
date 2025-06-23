/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.config;

import lombok.Getter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;


@Getter
public abstract class DatabaseCommandConfig<T> {
    protected final DatabaseConnectConfig dbConnectConfig;
    protected final DatabaseTypeEnum dbTypeEnum;
    protected final T commandConfig;

    public DatabaseCommandConfig(DatabaseConnectConfig dbConnectConfig, DatabaseTypeEnum dbTypeEnum, T commandConfig) {
        this.dbConnectConfig = dbConnectConfig;
        this.dbTypeEnum = dbTypeEnum;
        this.commandConfig = commandConfig;
    }

    public abstract String taskRunSQL();

    public abstract Schema getResultSchema();
}
