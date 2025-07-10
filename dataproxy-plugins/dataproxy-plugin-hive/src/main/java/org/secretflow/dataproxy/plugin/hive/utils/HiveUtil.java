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

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.utils.PartitionSpec;

import java.sql.*;
import java.util.*;
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
        String sql =  "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause);
        log.info("buildQuerySql sql:{}", sql);
        return sql;
    }

    public static String buildCreateTableSql(String tableName, Schema schema, PartitionSpec partitionSpec) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partitionSpec.keys(); // 分区字段名集合

        // 用于快速通过字段名查找 Field
        Map<String, Field> fieldMap = new LinkedHashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }

        // 表字段（不包含分区字段）
        boolean first = true;
        for (Field field : fields) {
            String fieldName = field.getName();
            if (partitionKeys.contains(fieldName)) {
                continue; // 跳过分区字段
            }

            if (!first) {
                sb.append(",\n");
            }
            sb.append("  ").append(fieldName)
                    .append(" ")
                    .append(arrowTypeStrToJdbcType(field.getType().toString()));
            first = false;
        }
        sb.append("\n)");

        // 分区字段（需要类型，但类型从 schema 中查）
        if (!partitionKeys.isEmpty()) {
            sb.append("\nPARTITIONED BY (\n");
            first = true;
            for (String partKey : partitionKeys) {
                Field partitionField = fieldMap.get(partKey);
                if (partitionField == null) {
                    log.error("Partition column '" + partKey + "' not found in schema");
                    throw new IllegalArgumentException("Partition column '" + partKey + "' not found in schema");
                }

                if (!first) {
                    sb.append(",\n");
                }
                sb.append("  ").append(partKey)
                        .append(" ")
                        .append(arrowTypeStrToJdbcType(partitionField.getType().toString()));
                first = false;
            }
            sb.append("\n)");
        }

        log.info("buildCreateTableSql sql:{}", sb);
        return sb.toString();
    }

    public static String buildInsertSql(String tableName, Schema schema, Map<String, Object> data, PartitionSpec partitionSpec) {
        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partitionSpec.keys();

        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();

        // 遍历 schema 中的字段（以保持字段顺序）
        for (Field field : fields) {
            String fieldName = field.getName();

            // 分区字段单独处理
            if (partitionKeys.contains(fieldName)) {
                continue;
            }

            columns.add(fieldName);
            Object rawValue = data.get(fieldName);
            ArrowType arrowType = field.getType();

            values.add(formatValue(rawValue, arrowType));
        }

        // 构建 PARTITION 字段
        List<String> partitionClauses = new ArrayList<>();
        for (String partKey : partitionKeys) {
            Object partVal = data.get(partKey); // 注意：从 data 中获取值更安全
            Field field = fields.stream()
                    .filter(f -> f.getName().equals(partKey))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Partition key not found in schema: " + partKey));

            String formatted = formatValue(partVal, field.getType());
            partitionClauses.add(partKey + "=" + formatted);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO TABLE ").append(tableName);
        if (!partitionClauses.isEmpty()) {
            sb.append(" PARTITION (").append(String.join(", ", partitionClauses)).append(")");
        }

        sb.append(" (").append(String.join(", ", columns)).append(")");
        sb.append(" VALUES (").append(String.join(", ", values)).append(")");
        log.info("buildInsertSql sql: {}", sb);
        return sb.toString();
    }

    // 根据 ArrowType 和值格式化（如加引号）
    private static String formatValue(Object value, ArrowType type) {
        if (value == null) {
            return "NULL";
        }

        return switch (type.getTypeID()) {
            case Utf8, Binary, FixedSizeBinary -> "'" + escapeString(value.toString()) + "'";
            case Int, FloatingPoint, Bool -> value.toString();
            case Date, Timestamp, Time -> "'" + value.toString() + "'";
            case Decimal -> value.toString();  // 可扩展加精度判断
            default -> "'" + escapeString(value.toString()) + "'";
        };
    }

    private static String escapeString(String str) {
        return str.replace("'", "''"); // Hive 中单引号转义为两个单引号
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

//    public static String arrowField2JdbcType(Field field) {
//        return switch (field.getFieldType().getType().getTypeID()) {
//            case Utf8 -> "VARCHAR(255)";
//            case Int -> switch (((ArrowType.Int)(field.getFieldType().getType())).getBitWidth()) {
//                case 8 -> "TINYINT";
//                case 16 -> "SMALLINT";
//                case 32 -> "INT";
//                case 64 -> "BIGINT";
//                default ->
//                        throw new IllegalArgumentException("Unexpected INT value BitWidth: " + ((ArrowType.Int)(field.getFieldType().getType())).getBitWidth());
//            };
//            case FloatingPoint -> switch (((ArrowType.FloatingPoint)(field.getFieldType().getType())).getPrecision()){
//                case HALF, SINGLE -> "FLOAT";
//                case DOUBLE -> "DOUBLE";
//            };
//            case Bool -> "BOOLEAN";
//            case Date -> "DATE";
//            case Time -> "TIME";
//            case Timestamp -> "TIMESTAMP";
//            case Decimal -> "DECIMAL(10, 2)";
//            case Binary -> "BLOB";
//            case FixedSizeBinary -> "BINARY";
//            default -> {
//                log.warn("Not Implemented type: {}", field.getFieldType().getType().getTypeID());
//                throw new IllegalArgumentException("Unexpected arrow field type: "+ field.getFieldType().getType().getTypeID());
//            }
//        };
//    }

    public static String arrowTypeStrToJdbcType(String arrowTypeStr) {
        if (arrowTypeStr == null || arrowTypeStr.isEmpty()) {
            throw new IllegalArgumentException("Arrow type string is null or empty");
        }

        arrowTypeStr = arrowTypeStr.trim();

        if (arrowTypeStr.startsWith("Utf8")) {
            return "VARCHAR(255)";
        } else if (arrowTypeStr.startsWith("Int")) {
            // 匹配 Int(bitWidth, isSigned)
            if (arrowTypeStr.contains("8")) {
                return "TINYINT";
            } else if (arrowTypeStr.contains("16")) {
                return "SMALLINT";
            } else if (arrowTypeStr.contains("32")) {
                return "INT";
            } else if (arrowTypeStr.contains("64")) {
                return "BIGINT";
            } else {
                throw new IllegalArgumentException("Unexpected INT value: " + arrowTypeStr);
            }
        } else if (arrowTypeStr.startsWith("FloatingPoint")) {
            if (arrowTypeStr.contains("SINGLE") || arrowTypeStr.contains("HALF")) {
                return "FLOAT";
            } else if (arrowTypeStr.contains("DOUBLE")) {
                return "DOUBLE";
            } else {
                throw new IllegalArgumentException("Unexpected FloatingPoint precision: " + arrowTypeStr);
            }
        } else if (arrowTypeStr.startsWith("Bool")) {
            return "BOOLEAN";
        } else if (arrowTypeStr.startsWith("Date")) {
            return "DATE";
        } else if (arrowTypeStr.startsWith("Time")) {
            return "TIME";
        } else if (arrowTypeStr.startsWith("Timestamp")) {
            return "TIMESTAMP";
        } else if (arrowTypeStr.startsWith("Decimal")) {
            return "DECIMAL(10, 2)";
        } else if (arrowTypeStr.startsWith("Binary")) {
            return "BLOB";
        } else if (arrowTypeStr.startsWith("FixedSizeBinary")) {
            return "BINARY";
        } else {
            throw new IllegalArgumentException("Unsupported Arrow type string: " + arrowTypeStr);
        }
    }

    public static boolean checkTableExists(Connection connection, String tableName) {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement("SHOW TABLES ?");
            stmt.setString(1, tableName);
            rs = stmt.executeQuery();
            boolean exists = rs.next();
            rs.close();
            stmt.close();
            return exists;
        } catch (SQLException e) {
            log.error("check whether table has existed error: {}", e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                log.error("close result or preparedStatement error: {}", e.getMessage());
            }
        }
    }


}
