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

package org.secretflow.dataproxy.plugin.hive.converter;

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
import org.secretflow.dataproxy.plugin.database.converter.DatabaseParamConverter;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveParamConverterTest {
    @InjectMocks
    private DatabaseParamConverter hiveParamConverter;

    @Mock
    private Flightinner.CommandDataMeshSqlQuery meshSqlQuery;

    @Mock
    private Flightinner.CommandDataMeshQuery meshQuery;

    @Mock
    private Flightinner.CommandDataMeshUpdate meshUpdate;

    private final Domaindatasource.DatabaseDataSourceInfo hiveDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo
                    .newBuilder()
                    .setEndpoint("endpoint")
                    .setDatabase("database")
                    .setUser("user")
                    .setPassword("password")
                    .build();
    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setDatabase(hiveDataSourceInfo).build();

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

        ScqlCommandJobConfig result = hiveParamConverter.convert(meshSqlQuery);
        assertNotNull(result);
        assertEquals(DatabaseTypeEnum.SQL, result.getDbTypeEnum());
        assertEquals(testSql, result.taskRunSQL());

        this.testHiveConnectConfig(result.getDbConnectConfig());
    }


    /**
     * Test scenario: Test the convert method, enter the CommandDataMeshQuery type, and the ContentType to CSV
     */
    @Test
    public void testConvertMeshQueryWithCsvContentType() {
        when(meshQuery.getQuery()).thenReturn(commandDomainDataQueryWithCsv);
        when(meshQuery.getDomaindata()).thenReturn(domainData);
        when(meshQuery.getDatasource()).thenReturn(domainDataSource);

        DatabaseTableQueryConfig result = this.testConvertMeshQueryWithNonType();
        assertEquals(DatabaseTypeEnum.TABLE, result.getDbTypeEnum());
    }


    /**
     * Test Scenario: Test the convert method, enter the CommandDataMeshUpdate type, and the ContentType to CSV
     */
    @Test
    public void testConvertMeshUpdateWithCsvContentType() {

        when(meshUpdate.getUpdate()).thenReturn(commandDomainDataUpdateWithCsv);
        when(meshUpdate.getDomaindata()).thenReturn(domainData);
        when(meshUpdate.getDatasource()).thenReturn(domainDataSource);

        DatabaseWriteConfig odpsWriteConfig = this.testConvertMeshUpdateWithNonType();
        assertEquals(DatabaseTypeEnum.TABLE, odpsWriteConfig.getDbTypeEnum());
    }

    private void testHiveConnectConfig(DatabaseConnectConfig hiveConnectConfig) {
        assertNotNull(hiveConnectConfig);
        assertEquals("endpoint", hiveConnectConfig.endpoint());
        assertEquals("database", hiveConnectConfig.database());
        assertEquals("user", hiveConnectConfig.username());
        assertEquals("password", hiveConnectConfig.password());
    }

    private DatabaseTableQueryConfig testConvertMeshQueryWithNonType() {
        DatabaseTableQueryConfig result = hiveParamConverter.convert(meshQuery);
        assertNotNull(result);

        this.testHiveConnectConfig(result.getDbConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }

    private DatabaseWriteConfig testConvertMeshUpdateWithNonType() {
        DatabaseWriteConfig result = hiveParamConverter.convert(meshUpdate);
        assertNotNull(result);

        this.testHiveConnectConfig(result.getDbConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }
}
