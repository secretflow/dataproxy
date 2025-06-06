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

package org.secretflow.dataproxy.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * @author yuexie
 * @date 2024/12/6 16:16
 **/
@Slf4j
public class EnvVarUtils {


    public static Optional<Integer> getInt(String key) {

        String env = System.getenv(key);
        if (env == null || env.isEmpty()) {
            return Optional.empty();
        }

        if (!env.matches("-?\\d+")) {
            log.warn("Env var `{}` is not a valid integer: {}", key, env);
            return Optional.empty();
        }
        return Optional.of(Integer.parseInt(env));
    }


    public static Optional<Long> getLong(String key) {
        String env = System.getenv(key);
        if (env == null || env.isEmpty()) {
            return Optional.empty();
        }

        if (!env.matches("-?\\d+")) {
            log.warn("Env var `{}` is not a valid long: {}", key, env);
            return Optional.empty();
        }
        return Optional.of(Long.parseLong(env));
    }

    public static int getEffectiveValue(int v, int minValue, int maxValue) {
        if (v <= minValue) {
            return minValue;
        }
        return Math.min(v, maxValue);
    }

    public static long getEffectiveValue(long v, long minValue, long maxValue) {
        if (v <= minValue) {
            return minValue;
        }
        return Math.min(v, maxValue);
    }
}
