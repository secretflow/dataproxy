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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class OracleUtil {
    public static Connection initOracle(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        int port = 1521; // default port

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
            if(!config.username().isEmpty() && !config.password().isEmpty()) {
                conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%d:%s", ip, port, config.database()), config.username(), config.password());
            } else {
                conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%d:%s", ip, port, config.database()));
            }
        } catch (Exception e) {
            log.error("database init error \"{}\"", e.getMessage());
            throw new RuntimeException(e);
        }
        return conn;

    }

    public static String buildQuerySql(String tableName, List<String> fields, String whereClause) {
        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        log.info("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from \"" + tableName +"\"";
    }

    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        switch (jdbcType.toLowerCase()) {
            case "int":
            case "integer":
                return Types.MinorType.INT.getType();
            case "bigint":
                return Types.MinorType.BIGINT.getType();
            case "float":
                return Types.MinorType.FLOAT4.getType();
            case "double":
                return Types.MinorType.FLOAT8.getType();
            case "varchar":
            case "string":
                Types.MinorType.VARCHAR.getType();
            case "boolean":
                Types.MinorType.BIT.getType();
            case "date":
                Types.MinorType.DATEDAY.getType();
            case "timestamp":
                Types.MinorType.TIMESTAMPMILLI.getType();
            default:
                throw new IllegalArgumentException("Unsupported JDBC type: " + jdbcType);
        }
    }

    public static String wrapTableName(String tableName) {
        return "\"" + tableName + "\"";
    }

    public static String arrowField2JdbcType(Field field) {
        return switch (field.getFieldType().getType().getTypeID()) {
            case Int -> "INT";
            case FloatingPoint -> "FLOAT";
            case Bool -> "BOOLEAN";
            case Date -> "DATE";
            case Time -> "TIME";
            case Timestamp -> "TIMESTAMP";
            case Decimal -> "DECIMAL(10, 2)";
            case Binary -> "BLOB";
            case FixedSizeBinary -> "BINARY";
            default -> "VARCHAR(255)";
        };
    }

    public static Boolean checkTableExists(Connection connection, String tableName) {
        String sql = "SELECT COUNT(*) FROM all_tables WHERE table_name = '" + tableName + "'";
        try {
            Statement stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            boolean exists = rs.getInt(1) > 0;
            rs.close();
            stmt.close();
            return exists;
        } catch (Exception e) {
            log.error("check whether table has existed sql:{} error: " + e.getMessage(), sql);
            throw new RuntimeException(e);
        }
    }
}
