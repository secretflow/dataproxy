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

package org.secretflow.dataproxy.plugin.database.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseTableQueryConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.config.ScqlCommandJobConfig;
import org.secretflow.dataproxy.plugin.database.constant.DatabaseTypeEnum;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatabaseParamConverterTest {

    @InjectMocks
    private DatabaseParamConverter dbParamConverter;

    @Mock
    private Flightinner.CommandDataMeshSqlQuery meshSqlQuery;

    @Mock
    private Flightinner.CommandDataMeshQuery meshQuery;

    @Mock
    private Flightinner.CommandDataMeshUpdate meshUpdate;

    private final Domaindatasource.DatabaseDataSourceInfo dbDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo
                    .newBuilder()
                    .setEndpoint("endpoint")
                    .setDatabase("database")
                    .setUser("user")
                    .setPassword("password")
                    .build();
    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setDatabase(dbDataSourceInfo).build();

    private final Domaindatasource.DomainDataSource domainDataSource =
            Domaindatasource.DomainDataSource.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("datasourceName")
                    .setType("hive")
                    .setInfo(dataSourceInfo)
                    .build();

    private final Domaindata.DomainData domainData =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("domainDataName")
                    .setRelativeUri("table_name_or_file_path")
                    .setDomaindataId("domainDataId")
                    .setType("table")
                    .build();
    private final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCsv =
            Flightdm.CommandDomainDataQuery.newBuilder()
                    .setContentType(Flightdm.ContentType.CSV)
                    .setPartitionSpec("partition_spec")
                    .build();

    private final Flightdm.CommandDomainDataUpdate commandDomainDataUpdateWithCsv =
            Flightdm.CommandDomainDataUpdate.newBuilder()
                    .setContentType(Flightdm.ContentType.CSV)
                    .setPartitionSpec("partition_spec")
                    .build();


    /**
     * Test Scenario: Test the convert method and enter the CommandDataMeshSqlQuery type
     */
    @Test
    public void testConvertMeshSqlQuery() {
        String testSql = "select * from test_table;";
        final Flightdm.CommandDataSourceSqlQuery sqlQuery = Flightdm.CommandDataSourceSqlQuery.newBuilder()
                .setDatasourceId("dataSourceId")
                .setSql(testSql).build();
        when(meshSqlQuery.getQuery()).thenReturn(sqlQuery);
        when(meshSqlQuery.getDatasource()).thenReturn(domainDataSource);

        ScqlCommandJobConfig result = dbParamConverter.convert(meshSqlQuery);
        assertNotNull(result);
        assertEquals(DatabaseTypeEnum.SQL, result.getDbTypeEnum());
        assertEquals(testSql, result.taskRunSQL());

        this.testDatabaseConnectConfig(result.getDbConnectConfig());
    }

    private void testDatabaseConnectConfig(DatabaseConnectConfig hiveConnectConfig) {
        assertNotNull(hiveConnectConfig);
        assertEquals("password", hiveConnectConfig.password());
        assertEquals("user", hiveConnectConfig.username());
        assertEquals("database", hiveConnectConfig.database());
        assertEquals("endpoint", hiveConnectConfig.endpoint());
    }

    private DatabaseTableQueryConfig testConvertMeshQueryWithNonType() {
        DatabaseTableQueryConfig result = dbParamConverter.convert(meshQuery);
        assertNotNull(result);

        this.testDatabaseConnectConfig(result.getDbConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }


    private DatabaseWriteConfig testConvertMeshUpdateWithNonType() {
        DatabaseWriteConfig result = dbParamConverter.convert(meshUpdate);
        assertNotNull(result);

        this.testDatabaseConnectConfig(result.getDbConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }
}
