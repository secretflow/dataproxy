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
 * @date 2024/11/1 20:25
 **/
public class ByteValueVisitor implements ValueVisitor<Byte>{

    @Override
    public Byte visit(boolean value) {
        return value? (byte)1: (byte)0;
    }

    @Override
    public Byte visit(@Nonnull Short value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Integer value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Long value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Float value) {
        return value.byteValue();
    }

    @Override
    public Byte visit(@Nonnull Double value) {
        return value.byteValue();
    }

}
