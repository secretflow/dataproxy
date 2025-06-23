package org.secretflow.dataproxy.plugin.database.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.Field;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;

@Slf4j
public class OracleRecordWriter extends AbstractDatabaseRecordWriter{

    public OracleRecordWriter(DatabaseWriteConfig commandConfig, Function<DatabaseConnectConfig, Connection> initFunc) {
        super(commandConfig, initFunc);
    }

    @Override
    protected String wrapTableName(String tableName) {
        return "\"" + tableName + "\"";
    }

    @Override
    protected String getJdbcType(Field field) {
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

    @Override
    protected boolean isExistsTable(Connection connection, String tableName) {
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
