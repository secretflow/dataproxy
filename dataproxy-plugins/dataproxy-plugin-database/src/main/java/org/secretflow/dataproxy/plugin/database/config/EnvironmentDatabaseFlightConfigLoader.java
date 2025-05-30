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

package org.secretflow.dataproxy.plugin.database.config;

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.common.utils.EnvVarUtils;
import org.secretflow.dataproxy.core.config.ConfigLoader;

import java.util.Optional;
import java.util.Properties;

/**
 * @author yuexie
 * @date 2024/12/6 15:45
 **/
@Slf4j
public class EnvironmentDatabaseFlightConfigLoader implements ConfigLoader {

    /**
     * load properties<br>
     * Read the information and load it into the properties passed in<br>
     * The reading order is sorted by {@link #getPriority()}<br>
     *
     * @param properties properties
     */
    @Override
    public void loadProperties(Properties properties) {

        try {

            Optional<Long> lifeCycle = EnvVarUtils.getLong(DatabaseConfigConstant.ConfigKey.HIVE_TABLE_LIFECYCLE_VALUE);
            if (lifeCycle.isPresent()) {
                log.debug("Load database flight config `DATABASE_TABLE_LIFECYCLE_VALUE` from system env, limits range 1 to 37231. key: {}, value:{}", DatabaseConfigConstant.ConfigKey.HIVE_TABLE_LIFECYCLE_VALUE, lifeCycle.get());
                properties.put(DatabaseConfigConstant.ConfigKey.HIVE_TABLE_LIFECYCLE_VALUE, EnvVarUtils.getEffectiveValue(lifeCycle.get(), 1L, 37231L));
            }

        } catch (Exception e) {
            log.error("Failed to load database flight config from system env. This error will be ignored and some configurations will not take effect. error: {}", e.getMessage(), e);
        }

    }

    @Override
    public int getPriority() {
        return 3;
    }


}
