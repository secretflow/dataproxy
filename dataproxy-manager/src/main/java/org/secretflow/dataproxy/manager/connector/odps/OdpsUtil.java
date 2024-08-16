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
package org.secretflow.dataproxy.manager.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import org.secretflow.dataproxy.common.model.datasource.conn.OdpsConnConfig;

/**
 * odps util
 *
 * @author yuexie
 * @date 2024-06-01 17:08:45
 */
public class OdpsUtil {

    public static Odps buildOdps(OdpsConnConfig odpsConnConfig) {
        Account account = new AliyunAccount(odpsConnConfig.getAccessKeyId(), odpsConnConfig.getAccessKeySecret());
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsConnConfig.getEndpoint());
        odps.setDefaultProject(odpsConnConfig.getProjectName());

        return odps;
    }
}
