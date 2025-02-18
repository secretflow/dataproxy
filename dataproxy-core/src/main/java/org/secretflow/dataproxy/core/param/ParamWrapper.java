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

package org.secretflow.dataproxy.core.param;

import javax.annotation.Nullable;

/**
 * @author yuexie
 * @date 2024/10/31 15:59
 **/
public final class ParamWrapper {

    private final String producerKey;
    private volatile Object param;

    public ParamWrapper(String producerKey, Object param) {
        this.producerKey = producerKey;
        this.param = param;
    }

    public String producerKey() {
        return producerKey;
    }

    public Object param() throws InterruptedException {
        if (param == null ) {
            synchronized (this) {
                if (param == null) {
                    this.wait();
                }
            }
        }
        return param;
    }

    public Object param(long timeoutMillis) throws InterruptedException {

        if (param == null) {
            synchronized (this) {
                if (param == null) {
                    this.wait(timeoutMillis);
                }
            }
        }
        return param;
    }

    /**
     * set param <br>
     * - if param is null, set it to realValue<br>
     * - if param is not null, throw exception
     *
     * @param realValue real value
     * @throws IllegalArgumentException if param `realValue` is null.
     */
    public void setParamIfAbsent(Object realValue) throws IllegalArgumentException {

        if (this.param == null) {
            synchronized (this) {
                if (this.param == null) {
                    if (realValue == null) {
                        throw new IllegalArgumentException("realValue is null");
                    }
                    this.param = realValue;
                    this.notifyAll();
                } else {
                    throw new IllegalArgumentException("param is not allowed to be modified");
                }
            }
        } else {
            throw new IllegalArgumentException("param is not allowed to be modified");
        }
    }

    /**
     * of param
     *
     * @param param param
     * @return param wrapper
     */
    public static ParamWrapper of(String producerKey, @Nullable Object param) {
        return new ParamWrapper(producerKey, param);
    }

    /**
     * unwrap param
     *
     * @param distClas dist class
     * @param <D>      dist class type
     * @return dist class
     */
    public <D> D unwrap(Class<D> distClas) {
        if (param == null) {
            throw new IllegalArgumentException("param is null");
        }

        if (!distClas.isAssignableFrom(param.getClass())) {
            throw new IllegalArgumentException("param is not assignable from " + distClas.getName());
        }
        return distClas.cast(param);
    }
}
