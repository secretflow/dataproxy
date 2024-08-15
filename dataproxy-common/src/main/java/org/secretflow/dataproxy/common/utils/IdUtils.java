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

import okio.ByteString;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * @author chengyuan.mc
 * @date 2021/9/2 11:57 上午
 **/
public class IdUtils {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final Random random = new Random();

    /**
     * 随机字母表
     */
    private static final String idLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /**
     * 随机串长度
     */
    private static final int idLen = 8;

    /**
     * 生成ID
     *
     * @param prefix,   前缀
     * @param splitter, 分隔符
     * @return
     */
    public static String createId(String prefix, String splitter) {
        String dateText;
        synchronized (dateFormat) {
            dateText = dateFormat.format(new Date());
        }
        return prefix + splitter + dateText + splitter + createRandString(idLen);
    }

    /**
     * 生成随机字符串
     *
     * @return
     */
    public static String createRandString(int len) {
        char[] idChars = new char[len];
        for (int i = 0; i < len; i++) {
            idChars[i] = idLetters.charAt(random.nextInt(idLetters.length()));
        }
        return new String(idChars);
    }

    /**
     * 生成随机uuid
     *
     * @return
     */
    public static String randomUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 多个id生成联合id
     *
     * @param ids
     * @return
     */
    public static String combineIds(String... ids) {
        return ByteString
            .encodeUtf8(StringUtils.join(ids, "|"))
            .sha256()
            .hex();
    }

    /**
     * 拼接两个 id 作为 traceId
     */
    public static String concatIds(String id1, String id2) {
        return id1 + "|" + id2;
    }

    /**
     * 拆分出 traceId 的两个 id
     */
    public static String[] splitIds(String str) {
        return str.split("|");
    }

    /**
     * 压缩 uuid 长度
     */
    public static String compressUUID(String uuid) {
        String hex = uuid.replace("-", "");
        byte[] bytes = hex2Bytes(hex);
        return Base64.getEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] hex2Bytes(String hex) {
        if (hex == null || hex.isEmpty()) {
            return new byte[0];
        }
        byte[] bytes = hex.getBytes();
        int n = bytes.length >> 1;
        byte[] buf = new byte[n];
        for (int i = 0; i < n; i++) {
            int index = i << 1;
            buf[i] = (byte) ((byte2Int(bytes[index]) << 4) | byte2Int(bytes[index + 1]));
        }
        return buf;
    }

    private static int byte2Int(byte b) {
        return (b <= '9') ? b - '0' : b - 'a' + 10;
    }

}
