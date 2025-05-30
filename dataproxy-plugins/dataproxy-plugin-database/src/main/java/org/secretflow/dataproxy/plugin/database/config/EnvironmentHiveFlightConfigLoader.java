package org.secretflow.dataproxy.plugin.database.config;

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.common.utils.EnvVarUtils;
import org.secretflow.dataproxy.core.config.ConfigLoader;

import java.util.Optional;
import java.util.Properties;

@Slf4j
public class EnvironmentHiveFlightConfigLoader implements ConfigLoader {
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
                log.debug("Load database flight config `HIVE_TABLE_LIFECYCLE_VALUE` from system env, limits range 1 to 37231. key: {}, value:{}", DatabaseConfigConstant.ConfigKey.HIVE_TABLE_LIFECYCLE_VALUE, lifeCycle.get());
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
