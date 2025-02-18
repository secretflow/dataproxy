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

package org.secretflow.dataproxy.core.visitor;

import javax.annotation.Nonnull;

/**
 * @author yuexie
 * @date 2024/11/1 20:22
 **/
public class DoubleValueVisitor implements ValueVisitor<Double> {

    @Override
    public Double visit(boolean value) {
        return value ? 1.0 : 0.0;
    }

    @Override
    public Double visit(@Nonnull Short value) {
        return value.doubleValue();
    }

    @Override
    public Double visit(@Nonnull Integer value) {
        return value.doubleValue();
    }

    @Override
    public Double visit(@Nonnull Long value) {
        return value.doubleValue();
    }

    @Override
    public Double visit(@Nonnull Float value) {
        return value.doubleValue();
    }

    @Override
    public Double visit(@Nonnull Double value) {
        return value;
    }

    @Override
    public Double visit(@Nonnull String value) {
        return Double.valueOf(value);
    }

    @Override
    public Double visit(@Nonnull Object value) {
        if (value instanceof Double doubleValue) {
            return doubleValue;
        }
        return Double.valueOf(value.toString());
    }
}
