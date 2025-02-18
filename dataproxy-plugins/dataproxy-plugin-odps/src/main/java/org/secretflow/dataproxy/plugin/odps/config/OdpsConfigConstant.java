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

package org.secretflow.dataproxy.plugin.odps.config;

/**
 * @author yuexie
 * @date 2024/11/28 17:27
 **/
public class OdpsConfigConstant {

    public static class ConfigKey {

        /**
         * If this value is set, the table is created according to this value.<br>
         * if it is not set, it is empty by default, and if ODPS must require a lifecycle and the value is not set, the creation fails. <br>
         *
         * This value is the value of the ODPS table lifecycle, and the unit is days.<br>
         * 1~37231, the default value is 37231.<br>
         * The value of this parameter is used to determine whether the table is expired.<br>
         * If the table is expired, it will be deleted.
         */
        public static final String ODPS_TABLE_LIFECYCLE_VALUE = "ODPS_TABLE_LIFECYCLE_VALUE";

    }
}
