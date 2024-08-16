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

package org.secretflow.dataproxy.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author muhong
 * @date 2023-10-19 11:13
 */
public class DPStringUtils {

    /**
     * 去前后包装标识
     *
     * @param origin     原始字符串
     * @param identifier 包装标识
     * @return
     */
    public static String removeDecorateIdentifier(String origin, String identifier) {
        String removeStart = StringUtils.removeStart(origin, identifier);
        return StringUtils.removeEnd(removeStart, identifier);
    }

    /**
     * 忽略空值拼接
     *
     * @param delimiter 间隔符
     * @param array     待拼接数组
     * @return
     */
    public static String joinWithoutEmpty(String delimiter, String... array) {
        if (array == null || array.length == 0) {
            return "";
        }

        List<Object> notEmptyList = Arrays.stream(array).filter(Objects::nonNull).collect(Collectors.toList());
        if (notEmptyList.isEmpty()) {
            return "";
        }

        return StringUtils.join(notEmptyList, delimiter);
    }

}
