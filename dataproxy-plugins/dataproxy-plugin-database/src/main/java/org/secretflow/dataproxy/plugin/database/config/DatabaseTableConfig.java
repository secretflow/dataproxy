package org.secretflow.dataproxy.plugin.database.config;


import com.fasterxml.jackson.annotation.JsonIgnore;
import org.secretflow.v1alpha1.common.Common;

import java.util.List;

public record DatabaseTableConfig(String tableName, String partition, @JsonIgnore List<Common.DataColumn> columns) {
}
