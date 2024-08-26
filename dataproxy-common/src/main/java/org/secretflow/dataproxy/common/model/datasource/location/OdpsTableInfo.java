/*
 * Copyright 2024 Ant Group Co., Ltd.
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
