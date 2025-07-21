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
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter.PasrsePartition;

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
                final Map<String, String> partitionSpec = PasrsePartition(whereClause);

                for (String key : partitionSpec.keySet()) {
                    if (!columnOrValuePattern.matcher(key).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition key:" + key);
                    }
                    if (!columnOrValuePattern.matcher(partitionSpec.get(key)).matches()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid partition value:" + partitionSpec.get(key));
                    }
                }

                List<String> list = partitionSpec.keySet().stream().map(k -> k + "='" + partitionSpec.get(k) + "'").toList();
                whereClause = String.join(" and ", list);
            }
        }
        String sql =  "select " + String.join(",", fields) + " from " + tableName + (whereClause.isEmpty() ? "" : " where " + whereClause);
        log.info("buildQuerySql sql:{}", sql);
        return sql;
    }

    public static String buildCreateTableSql(String tableName, Schema schema, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");

        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partition.keySet(); // 分区字段名集合

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
                    .append(arrowTypeToJdbcType(field.getType()));
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
                        .append(arrowTypeToJdbcType(partitionField.getType()));
                first = false;
            }
            sb.append("\n)");
        }

        log.info("buildCreateTableSql sql:{}", sb);
        return sb.toString();
    }

    public static String buildInsertSql(String tableName, Schema schema, Map<String, Object> data, Map<String, String> partition) {
        List<Field> fields = schema.getFields();
        Set<String> partitionKeys = partition.keySet();

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

    public static String buildMultiRowInsertSql(String tableName, Schema schema, List<Map<String, Object>> dataList, Map<String, String> partition) {
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("No data to insert");
        }

        Set<String> partitionKeys = partition.keySet();
        List<Field> fields = schema.getFields();

        // 取第一条数据判断 partition
        Map<String, Object> firstRow = dataList.get(0);
        List<String> partitionClauses = new ArrayList<>();
        for (String partKey : partitionKeys) {
            Object partVal = firstRow.get(partKey);
            Field field = fields.stream()
                    .filter(f -> f.getName().equals(partKey))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Partition key not found in schema: " + partKey));
            partitionClauses.add(partKey + "=" + formatValue(partVal, field.getType()));
        }

        // 非 partition 字段
        List<String> columns = fields.stream()
                .map(Field::getName)
                .filter(name -> !partitionKeys.contains(name))
                .collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO TABLE ").append(tableName);
        if (!partitionClauses.isEmpty()) {
            sb.append(" PARTITION (").append(String.join(", ", partitionClauses)).append(")");
        }
        sb.append(" (").append(String.join(", ", columns)).append(")");
        sb.append(" VALUES ");

        List<String> rows = new ArrayList<>();
        for (Map<String, Object> row : dataList) {
            List<String> vals = new ArrayList<>();
            for (String col : columns) {
                Object val = row.get(col);
                ArrowType type = schema.findField(col).getType();
                vals.add(formatValue(val, type));
            }
            rows.add("(" + String.join(", ", vals) + ")");
        }

        sb.append(String.join(",\n", rows));
        return sb.toString();
    }

    private static String formatValue(Object value, ArrowType type) {
        if (value == null) {
            return "NULL";
        }

        return switch (type.getTypeID()) {
            case Utf8, Binary, FixedSizeBinary -> "'" + escapeString(value.toString()) + "'";
            case Int, FloatingPoint, Bool -> value.toString();
            case Date, Timestamp, Time -> "'" + value + "'";
            case Decimal -> value.toString();
            default -> "'" + escapeString(value.toString()) + "'";
        };
    }

    private static String escapeString(String str) {
        return str.replace("'", "''");
    }

    public static ArrowType jdbcType2ArrowType(String jdbcType) {
        if (jdbcType == null || jdbcType.isEmpty()) {
            throw new IllegalArgumentException("Hive type is null or empty");
        }

        String type = jdbcType.trim().toLowerCase();

        // 处理 decimal(p,s)
        if (type.startsWith("decimal")) {
            Pattern pattern = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
            Matcher matcher = pattern.matcher(type);
            if (matcher.find()) {
                int precision = Integer.parseInt(matcher.group(1));
                int scale = Integer.parseInt(matcher.group(2));
                return new ArrowType.Decimal(precision, scale, 128);
            } else {
                return new ArrowType.Decimal(38, 10, 128); // 默认精度
            }
        }

        return switch (type) {
            case "tinyint" -> new ArrowType.Int(8, true);
            case "smallint" -> new ArrowType.Int(16, true);
            case "int", "integer" -> new ArrowType.Int(32, true);
            case "bigint" -> new ArrowType.Int(64, true);
            case "float" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "double" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "string", "varchar", "char" -> new ArrowType.Utf8();
            case "boolean" -> new ArrowType.Bool();
            case "date" -> new ArrowType.Date(DateUnit.DAY);
            case "timestamp" -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "binary" -> new ArrowType.Binary();

            default -> throw new IllegalArgumentException("Unsupported Hive type: " + jdbcType);
        };
    }

    public static String arrowTypeToJdbcType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Utf8) {
            return "STRING";
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            int bitWidth = intType.getBitWidth();
            boolean signed = intType.getIsSigned();
            switch (bitWidth) {
                case 8:
                    return signed ? "TINYINT" : "TINYINT UNSIGNED";
                case 16:
                    return signed ? "SMALLINT" : "SMALLINT UNSIGNED";
                case 32:
                    return signed ? "INT" : "INT UNSIGNED";
                case 64:
                    return signed ? "BIGINT" : "BIGINT UNSIGNED";
                default:
                    throw new IllegalArgumentException("Unsupported Int bitWidth: " + bitWidth);
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
            switch (fp.getPrecision()) {
                case SINGLE:
                    return "FLOAT";
                case DOUBLE:
                    return "DOUBLE";
                default:
                    throw new IllegalArgumentException("Unsupported floating point type");
            }
        } else if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        } else if (arrowType instanceof ArrowType.Date) {
            return "DATE";
        } else if (arrowType instanceof ArrowType.Time) {
            return "TIME";
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return "TIMESTAMP";
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal dec = (ArrowType.Decimal) arrowType;
            return "DECIMAL(" + dec.getPrecision() + ", " + dec.getScale() + ")";
        } else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.FixedSizeBinary) {
            return "BINARY";
        } else {
            throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType.getClass());
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
