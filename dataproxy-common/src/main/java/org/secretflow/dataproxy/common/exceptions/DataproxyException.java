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
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;

/**
 * dataproxy exception
 *
 * @author muhong
 * @date 2023-09-14 14:23
 */
@Getter
@Slf4j
public class DataproxyException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -9012364334166955517L;

    private final DataproxyErrorCode errorCode;

    public DataproxyException(DataproxyErrorCode errorCode) {
        super(errorCode.getErrorMessage());
        this.errorCode = errorCode;
    }

    public DataproxyException(DataproxyErrorCode errorCode, String message) {
        super(errorCode.getErrorMessage() + ": " + message);
        this.errorCode = errorCode;
    }

    public DataproxyException(DataproxyErrorCode errorCode, Throwable cause) {
        super(errorCode.getErrorMessage(), cause);
        this.errorCode = errorCode;
    }

    public DataproxyException(DataproxyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode.getErrorMessage() + ": " + message, cause);
        this.errorCode = errorCode;
    }

    public static DataproxyException of(DataproxyErrorCode errorCode) {
        return new DataproxyException(errorCode);
    }

    public static DataproxyException of(DataproxyErrorCode errorCode, String message) {
        return new DataproxyException(errorCode, message);
    }

    public static DataproxyException of(DataproxyErrorCode errorCode, Throwable cause) {
        return new DataproxyException(errorCode, cause);
    }

    public static DataproxyException of(DataproxyErrorCode errorCode, String message, Throwable cause) {
        return new DataproxyException(errorCode, message, cause);
    }

    public String getDescription() {
        return String.format("code: %s, message: %s", getErrorCode().getErrorCode(), getMessage());
    }
}
