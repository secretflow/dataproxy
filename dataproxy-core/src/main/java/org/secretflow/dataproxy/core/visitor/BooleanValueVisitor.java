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
 * @date 2024/11/1 20:08
 **/
public class BooleanValueVisitor implements ValueVisitor<Boolean>{

    @Override
    public Boolean visit(boolean value) {
        return value;
    }

    @Override
    public Boolean visit(@Nonnull Short value) {
        return value > 0;
    }

    @Override
    public Boolean visit(@Nonnull Integer value) {
        return value > 0;
    }

    @Override
    public Boolean visit(@Nonnull Long value) {
        return value > 0;
    }

    @Override
    public Boolean visit(@Nonnull Float value) {
        return value > 0;
    }

    @Override
    public Boolean visit(@Nonnull Double value) {
        return value > 0;
    }

    @Override
    public Boolean visit(@Nonnull String value) {
        return switch (value.toLowerCase()) {
            case "true", "t", "yes", "y", "1" -> true;

            case "false", "f", "no", "n", "0" -> false;

            default -> throw new IllegalStateException("BooleanValueVisitor unexpected String value: " + value);
        };
    }

    @Override
    public Boolean visit(@Nonnull Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return this.visit(value.toString());
    }
}
