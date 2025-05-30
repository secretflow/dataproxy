package org.secretflow.dataproxy.plugin.database.config;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;
import org.secretflow.dataproxy.common.utils.ArrowUtil;

import java.util.stream.Collectors;

public class DatabaseWriteConfig extends DatabaseCommandConfig<DatabaseTableConfig> {
    public DatabaseWriteConfig(DatabaseConnectConfig dbConnectConfig, DatabaseTableConfig readConfig) {
        super(dbConnectConfig, DatabaseTypeEnum.TABLE, readConfig);
    }

    public DatabaseWriteConfig(DatabaseConnectConfig dbConnectConfig, DatabaseTypeEnum typeEnum, DatabaseTableConfig readConfig) {
        super(dbConnectConfig, typeEnum, readConfig);
    }

    @Override
    public String taskRunSQL() {
        return "";
    }

    @Override
    public Schema getResultSchema() {
        return new Schema(commandConfig.columns().stream()
                .map(column ->
                        Field.nullable(column.getName(), ArrowUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));
    }
}
