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

package org.secretflow.dataproxy.common.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author yuexie
 * @date 2025/3/6 17:36
 **/
@ExtendWith({SystemStubsExtension.class})
public class EnvVarUtilsTest {

    @SystemStub
    private EnvironmentVariables environmentVariables;

    @BeforeEach
    public void setUp() {

        environmentVariables.set("EMPTY_KEY", "");
        environmentVariables.set("NON_INTEGER_KEY", "abc");
        environmentVariables.set("INTEGER_KEY", "123");

    }

    /**
     * Test Scenario: The getInt method is tested, and the environment variable is empty
     */
    @Test
    public void testGetIntWithEmptyEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getInt("EMPTY_KEY"));
    }

    /**
     * Test scenario: The getInt method is tested, and the environment variable does not exist
     */
    @Test
    public void testGetIntWithNotExistsEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getInt("NOT_EXISTS_KEY"));
    }

    /**
     * Test Scenario: The getInt method is tested, and the environment variable is a non-integer
     */
    @Test
    public void testGetIntWithNonIntegerEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getInt("NON_INTEGER_KEY"));
    }

    /**
     * Test scenario: The getInt method is tested, and the environment variable is an integer
     */
    @Test
    public void testGetIntWithIntegerEnv() {
        assertEquals(Optional.of(123), EnvVarUtils.getInt("INTEGER_KEY"));
    }

    /**
     * Test scenario: The getLong method is tested, and the environment variable is empty
     */
    @Test
    public void testGetLongWithEmptyEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getLong("EMPTY_KEY"));
    }

    /**
     * Test scenario: The getLong method is tested, and the environment variable does not exist
     */
    @Test
    public void testGetLongWithNotExistsEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getLong("NOT_EXISTS_KEY"));
    }

    /**
     * Test scenario: Test the getLong method, and the environment variable is a non-integer
     */
    @Test
    public void testGetLongWithNonIntegerEnv() {
        assertEquals(Optional.empty(), EnvVarUtils.getLong("NON_INTEGER_KEY"));
    }

    /**
     * Test Scenario: Test the getLong method and the environment variable is an integer
     */
    @Test
    public void testGetLongWithIntegerEnv() {
        assertEquals(Optional.of(123L), EnvVarUtils.getLong("INTEGER_KEY"));
    }

    /**
     * [Single Test Case] Test Scenario: Test the getEffectiveValue method, and the integer variable is less than the minimum value
     */
    @Test
    public void testGetEffectiveValueWithIntegerLessThanMin() {
        assertEquals(2, EnvVarUtils.getEffectiveValue(1, 2, 10));
    }

    /**
     * [Single Test Case] Test Scenario: Test the getEffectiveValue method, and the integer variable is greater than the maximum value
     */
    @Test
    public void testGetEffectiveValueWithIntegerGreaterThanMax() {
        assertEquals(10, EnvVarUtils.getEffectiveValue(11, 2, 10));
    }

    /**
     * Test scenario: Test the getEffectiveValue method, and the integer variable is between the minimum and maximum values
     */
    @Test
    public void testGetEffectiveValueWithIntegerBetweenMinAndMax() {
        assertEquals(5, EnvVarUtils.getEffectiveValue(5, 2, 10));
    }

    /**
     * Test scenario: The getEffectiveValue method is tested, and the long integer variable is less than the minimum value
     */
    @Test
    public void testGetEffectiveValueWithLongLessThanMin() {
        assertEquals(2L, EnvVarUtils.getEffectiveValue(1L, 2L, 10L));
    }

    /**
     * Test scenario: The getEffectiveValue method is tested, and the long integer variable is greater than the maximum value
     */
    @Test
    public void testGetEffectiveValueWithLongGreaterThanMax() {
        assertEquals(10L, EnvVarUtils.getEffectiveValue(11L, 2L, 10L));
    }

    /**
     * Test scenario: Test the getEffectiveValue method, and the long integer variable is between the minimum and maximum values
     */
    @Test
    public void testGetEffectiveValueWithLongBetweenMinAndMax() {
        assertEquals(5L, EnvVarUtils.getEffectiveValue(5L, 2L, 10L));
    }
}
