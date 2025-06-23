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
package org.secretflow.dataproxy.plugin.odps.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author 月榭
 * @date 2025/03/05 16:24:41
 */
@ExtendWith({MockitoExtension.class, SystemStubsExtension.class})
public class EnvironmentOdpsFlightConfigLoaderTest {

    @InjectMocks
    private EnvironmentOdpsFlightConfigLoader environmentOdpsFlightConfigLoader;

    @SystemStub
    private EnvironmentVariables environmentVariables;

    /**
     * Test Scenario: The test is normal, and there are ODPS_TABLE_LIFECYCLE_VALUE system environment variables
     */
    @Test
    public void testLoadPropertiesWithValidLifeCycle() {
        environmentVariables.set(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE, "10000");

        assertEquals("10000", System.getenv(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE));
        Properties properties = new Properties();
        environmentOdpsFlightConfigLoader.loadProperties(properties);
        Long lifeCycle = (Long) properties.get(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE);
        assertEquals(10000L, lifeCycle);
    }

    /**
     * Test Scenario: The test is abnormal, and the system environment variables do not exist ODPS_TABLE_LIFECYCLE_VALUE
     */
    @Test
    public void testLoadPropertiesWithInvalidLifeCycle() {
        Properties properties = new Properties();
        environmentOdpsFlightConfigLoader.loadProperties(properties);
        Long lifeCycle = (Long) properties.get(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE);
        assertNull(lifeCycle);
    }

    /**
     * Test Scenario: The test is abnormal and the system environment variables are ODPS_TABLE_LIFECYCLE_VALUE but the values exceed the maximum range
     */
    @Test
    public void testLoadPropertiesWithLargerThanRangeLifeCycle() {
        environmentVariables.set(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE, "40000");

        assertEquals("40000", System.getenv(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE));

        Properties properties = new Properties();
        environmentOdpsFlightConfigLoader.loadProperties(properties);
        Long lifeCycle = (Long) properties.get(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE);
        assertEquals(37231L, lifeCycle);
    }

    /**
     * Test scenario: The test is abnormal and the system environment variables are ODPS_TABLE_LIFECYCLE_VALUE but the values are outside the minimum range
     */
    @Test
    public void testLoadPropertiesWithLessThanRangeLifeCycle() {
        environmentVariables.set(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE, "-1");

        assertEquals("-1", System.getenv(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE));

        Properties properties = new Properties();
        environmentOdpsFlightConfigLoader.loadProperties(properties);
        Long lifeCycle = (Long) properties.get(OdpsConfigConstant.ConfigKey.ODPS_TABLE_LIFECYCLE_VALUE);
        assertEquals(1L, lifeCycle);
    }
}
