package org.secretflow.dataproxy.plugin.database.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.Field;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Function;

@Slf4j
public class DamengRecordWriter extends AbstractDatabaseRecordWriter{

    public DamengRecordWriter(DatabaseWriteConfig commandConfig, Function<DatabaseConnectConfig, Connection> initFunc) {
        super(commandConfig, initFunc);
    }

    @Override
    protected String wrapTableName(String tableName) {
        return "\"" + tableName + "\"";
    }

    @Override
    protected boolean isExistsTable(Connection connection, String tableName) {
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM dba_tables WHERE table_name = '" + tableName.toUpperCase() + "'");
            rs.next();
            boolean exists = rs.getInt(1) > 0;
            rs.close();
            stmt.close();
            return exists;
        } catch (Exception e) {
            log.error("check whether table has existed error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getJdbcType(Field field) {
        return switch (field.getFieldType().getType().getTypeID()) {
            case Int -> "INT";
            case FloatingPoint -> "FLOAT";
            case Bool -> "BIT";
            case Date -> "DATE";
            case Time -> "TIME";
            case Timestamp -> "TIMESTAMP";
            case Decimal -> "DECIMAL(10, 2)";
            case Binary -> "BLOB";
            case FixedSizeBinary -> "BINARY";
            default -> "VARCHAR(255)";
        };
    }
}
