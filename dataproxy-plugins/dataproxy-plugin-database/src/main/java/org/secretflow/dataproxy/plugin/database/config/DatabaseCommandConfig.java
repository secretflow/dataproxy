package org.secretflow.dataproxy.plugin.database.config;

import lombok.Getter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;


@Getter
public abstract class DatabaseCommandConfig<T> {
    protected final DatabaseConnectConfig dbConnectConfig;
    protected final DatabaseTypeEnum dbTypeEnum;
    protected final T commandConfig;

    public DatabaseCommandConfig(DatabaseConnectConfig dbConnectConfig, DatabaseTypeEnum dbTypeEnum, T commandConfig) {
        this.dbConnectConfig = dbConnectConfig;
        this.dbTypeEnum = dbTypeEnum;
        this.commandConfig = commandConfig;
    }

    public abstract String taskRunSQL();

    public abstract Schema getResultSchema();
}
