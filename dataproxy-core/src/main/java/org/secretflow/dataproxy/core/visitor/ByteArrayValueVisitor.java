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

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * @author yuexie
 * @date 2024/11/1 20:28
 **/
@Slf4j
public class ByteArrayValueVisitor implements ValueVisitor<byte[]>{

    @Override
    public byte[] visit(@Nonnull Short value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull Integer value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull Long value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull Float value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull Double value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull Date value) {
        return this.visit((Object) value);
    }

    @Override
    public byte[] visit(@Nonnull String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] visit(@Nonnull byte[] value) {
        return value;
    }

    @Override
    public byte[] visit(@Nonnull Object value) {

        if (value instanceof byte[] bytes) {
            return this.visit(bytes);
        }

        return this.visit(String.valueOf(value));
    }
}
