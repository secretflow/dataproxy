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

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Pb utils
 *
 * @author huanyu.wty(焕羽)
 * @date 2022/04/25
 */
public class ProtoBufJsonUtils {

    /**
     * Pb message to json string
     * <br>- Sensitive Information Risk
     * @param sourceMessage pb message
     * @return json string
     */
    public static String toJSONString(Message sourceMessage) {
        try {
            return JsonFormat.printer().print(sourceMessage);
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, e);
        }
    }

}
