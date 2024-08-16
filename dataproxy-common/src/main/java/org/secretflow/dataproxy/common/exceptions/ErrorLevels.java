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

package org.secretflow.dataproxy.common.exceptions;

import lombok.Getter;

/**
 * error levels
 *
 * @author muhong
 * @date 2023-09-14 14:22
 */
@Getter
public enum ErrorLevels {
    /**
     * INFO
     */
    INFO("1"),

    /**
     * WARN
     */
    WARN("3"),

    /**
     * ERROR
     */
    ERROR("5"),

    /**
     * FATAL
     */
    FATAL("7");

    private final String code;

    ErrorLevels(String code) {
        this.code = code;
    }
}
