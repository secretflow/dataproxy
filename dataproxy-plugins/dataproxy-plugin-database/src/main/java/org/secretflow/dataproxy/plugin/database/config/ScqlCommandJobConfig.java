package org.secretflow.dataproxy.plugin.database.config;

import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;


public class ScqlCommandJobConfig extends DatabaseCommandConfig<String> {
    public ScqlCommandJobConfig(DatabaseConnectConfig dbConnectConfig, String querySql) {
        super(dbConnectConfig, DatabaseTypeEnum.SQL, querySql);
    }

    @Override
    public String taskRunSQL(){
        return commandConfig;
    }

    @Override
    public Schema getResultSchema() {
        return null;
    }
}
