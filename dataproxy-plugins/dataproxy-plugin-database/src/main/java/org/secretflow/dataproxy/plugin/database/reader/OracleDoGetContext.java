package org.secretflow.dataproxy.plugin.database.reader;

import lombok.extern.slf4j.Slf4j;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.Connection;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

@Slf4j
public class OracleDoGetContext extends AbstractDatabaseDoGetContext{
    public OracleDoGetContext(DatabaseCommandConfig<?> config, Function<DatabaseConnectConfig, Connection> initDatabase) {
        super(config, initDatabase);
    }

    @Override
    protected String buildSql(String tableName, List<String> fields, String whereClause) {
        final Pattern columnOrValuePattern = Pattern.compile("^[\\u00b7A-Za-z0-9\\u4e00-\\u9fa5\\-_,.]*$");

        if (!columnOrValuePattern.matcher(tableName).matches()) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "Invalid tableName:" + tableName);
        }

        log.info("whereClause: {}", whereClause);

        return "select " + String.join(",", fields) + " from \"" + tableName +"\"";
    }
}
