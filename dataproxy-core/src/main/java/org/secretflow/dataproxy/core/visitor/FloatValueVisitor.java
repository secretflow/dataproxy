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
 * @date 2024/11/1 20:19
 **/
public class FloatValueVisitor implements ValueVisitor<Float>{
    @Override
    public Float visit(boolean value) {
        return value ? 1f : 0f;
    }

    @Override
    public Float visit(@Nonnull Short value) {
        return value.floatValue();
    }

    @Override
    public Float visit(@Nonnull Integer value) {
        return value.floatValue();
    }

    @Override
    public Float visit(@Nonnull Long value) {
        return value.floatValue();
    }

    @Override
    public Float visit(@Nonnull Float value) {
        return value;
    }

    @Override
    public Float visit(@Nonnull Double value) {
        return value.floatValue();
    }

    @Override
    public Float visit(@Nonnull String value) {
        return Float.valueOf(value);
    }

    @Override
    public Float visit(@Nonnull Object value) {

        if (value instanceof Float floatValue) {
            return this.visit(floatValue);
        }
        return Float.valueOf(String.valueOf(value));
    }
}
