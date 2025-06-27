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

package org.secretflow.dataproxy.plugin.hive.utils;

import com.aliyun.odps.PartitionSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.*;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class HiveUtil {
    
    public static Connection initHive(DatabaseConnectConfig config) {
        String endpoint = config.endpoint();
        String ip;
        int port = 10000; // default port

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
            // hive Authentication None
            if(!config.username().isEmpty() && !config.password().isEmpty()) {
                conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/%s", ip, port, config.database()), config.username(), config.password());
            } else {
                conn = DriverManager.getConnection(String.format("jdbc:hive2://%s:%s/%s", ip, port, config.database()));
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
        if (!whereClause.isEmpty()) {
            String[] groups = whereClause.split("[,/]");
            if (groups.length > 1) {
                final PartitionSpec partitionSpec = new PartitionSpec(whereClause);

                for (String key : partitionSpec.keys()) {
                    if (!columnOrValuePattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }
                    if (!columnOrValuePattern.matcher(partitionSpec.get(key)).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + partitionSpec.get(key));
                    }
                }

                List<String> list = partitionSpec.keys().stream().map(k -> k + "='" + partitionSpec.get(k) + "'").toList();
                whereClause = String.join(" and ", list);
            }
        }
        return "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause);
    }

    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        return switch (jdbcType.toLowerCase()) {
            case "int", "integer" -> Types.MinorType.INT.getType();
            case "bigint" -> Types.MinorType.BIGINT.getType();
            case "float" -> Types.MinorType.FLOAT4.getType();
            case "double" -> Types.MinorType.FLOAT8.getType();
            case "varchar", "string" -> Types.MinorType.VARCHAR.getType();
            case "boolean" -> Types.MinorType.BIT.getType();
            case "date" -> Types.MinorType.DATEDAY.getType();
            case "timestamp" -> Types.MinorType.TIMESTAMPMILLI.getType();
            default -> throw new IllegalArgumentException("Unsupported JDBC type: " + jdbcType);
        };
    }

    public static String wrapTableName(String tableName) {
        return tableName;
    }

    public static String arrowField2JdbcType(Field field) {
        return switch (field.getFieldType().getType().getTypeID()) {
            case Utf8 -> "VARCHAR(255)";
            case Int -> switch (((ArrowType.Int)(field.getFieldType().getType())).getBitWidth()) {
                case 8 -> "TINYINT";
                case 16 -> "SMALLINT";
                case 32 -> "INT";
                case 64 -> "BIGINT";
                default ->
                        throw new IllegalArgumentException("Unexpected INT value BitWidth: " + ((ArrowType.Int)(field.getFieldType().getType())).getBitWidth());
            };
            case FloatingPoint -> switch (((ArrowType.FloatingPoint)(field.getFieldType().getType())).getPrecision()){
                case HALF -> "FLOAT";
                case SINGLE -> "FLOAT";
                case DOUBLE -> "DOUBLE";
            };
            case Bool -> "BOOLEAN";
            case Date -> "DATE";
            case Time -> "TIME";
            case Timestamp -> "TIMESTAMP";
            case Decimal -> "DECIMAL(10, 2)";
            case Binary -> "BLOB";
            case FixedSizeBinary -> "BINARY";
            default -> {
                log.warn("Not Implemented type: {}", field.getFieldType().getType().getTypeID());
                throw new IllegalArgumentException("Unexpected arrow field type: "+ field.getFieldType().getType().getTypeID());
            }
        };
    }

    public static boolean checkTableExists(Connection connection, String tableName) {
        try {
            PreparedStatement stmt = connection.prepareStatement("SHOW TABLES '?'");
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            boolean exists = rs.next();
            rs.close();
            stmt.close();
            return exists;
        } catch (SQLException e) {
            log.error("check whether table has existed error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
