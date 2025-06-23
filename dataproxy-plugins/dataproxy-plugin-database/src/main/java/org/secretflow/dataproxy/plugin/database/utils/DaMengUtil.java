/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

@Slf4j
public class DaMengUtil {

    public static Connection initDaMeng(DatabaseConnectConfig config) {
        try{
            Class.forName("dm.jdbc.driver.DmDriver");
        } catch (ClassNotFoundException e) {
            log.error("dameng jdbc driver not found");
            throw new RuntimeException(e);
        }

        String endpoint = config.endpoint();
        String ip;
        int port = 5236; // 默认端口

        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }
        Connection conn;
        try{
            conn = DriverManager.getConnection(String.format("jdbc:dm://%s:%s/%s", ip, port, config.database()), config.username(), config.password());
        } catch (Exception e) {
            System.out.printf("database init error %s", e.getMessage());
            throw new RuntimeException(e);
        }

        return conn;

    }
}
