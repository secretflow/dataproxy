package org.secretflow.dataproxy.common.model.datasource.location;

import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;

import java.util.List;

public record OdpsTableInfo(String tableName, String partitionSpec, List<String> fields) implements LocationConfig {

    public static OdpsTableInfo fromKusciaData(Domaindata.DomainData domainData) {

        if (domainData.hasPartition() && !domainData.getPartition().getFieldsList().isEmpty()) {
            return new OdpsTableInfo(domainData.getRelativeUri(), domainData.getPartition().getFields(0).getName(), transformFields(domainData.getColumnsList()));
        }

        return new OdpsTableInfo(domainData.getRelativeUri(), "", transformFields(domainData.getColumnsList()));
    }

    private static List<String> transformFields(List<Common.DataColumn> columnList) {
        return columnList.stream().map(Common.DataColumn::getName).toList();
    }

}
