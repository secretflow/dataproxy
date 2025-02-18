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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * @author yuexie
 * @date 2024/11/1 16:48
 **/
public interface ValueVisitor<T> {

    default T visit(@Nonnull Integer value) {
        throw new UnsupportedOperationException("Integer not supported");
    }

    default T visit(@Nonnull Short value) {
        throw new UnsupportedOperationException("Short not supported");
    }

    default T visit(@Nonnull Long value) {
        throw new UnsupportedOperationException("Long not supported");
    }

    default T visit(@Nonnull Double value) {
        throw new UnsupportedOperationException("Double not supported");
    }

    default T visit(@Nonnull Float value) {
        throw new UnsupportedOperationException("Float not supported");
    }

    default T visit(boolean value) {
        throw new UnsupportedOperationException("Boolean not supported");
    }

    default T visit(@Nonnull Date value) {
        throw new UnsupportedOperationException("Date not supported");
    }

    default T visit(@Nonnull String value) {
        throw new UnsupportedOperationException("String not supported");
    }

    default T visit(@Nonnull byte[] value) {
        throw new UnsupportedOperationException("byte[] not supported");
    }

    default T visit(@Nonnull Object value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull ZonedDateTime value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull LocalDateTime value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull LocalDate value) {
        throw new UnsupportedOperationException("Object not supported");
    }

    default T visit(@Nonnull Instant value) {
        throw new UnsupportedOperationException("Object not supported");
    }

}
