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
package org.secretflow.dataproxy.plugin.odps.converter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableQueryConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsWriteConfig;
import org.secretflow.dataproxy.plugin.odps.config.ScqlCommandJobConfig;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * @author yuexie
 * @date 2025/2/12 14:08
 **/
@ExtendWith(MockitoExtension.class)
public class OdpsParamConverterTest {

    @InjectMocks
    private OdpsParamConverter odpsParamConverter;

    @Mock
    private Flightinner.CommandDataMeshSqlQuery meshSqlQuery;

    @Mock
    private Flightinner.CommandDataMeshQuery meshQuery;

    @Mock
    private Flightinner.CommandDataMeshUpdate meshUpdate;

    private final Domaindatasource.OdpsDataSourceInfo odpsDataSourceInfo =
            Domaindatasource.OdpsDataSourceInfo
                    .newBuilder()
                    .setAccessKeyId("accessKeyId")
                    .setAccessKeySecret("accessKeySecret")
                    .setProject("project")
                    .setEndpoint("endpoint")
                    .build();
    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setOdps(odpsDataSourceInfo).build();

    private final Domaindatasource.DomainDataSource domainDataSource =
            Domaindatasource.DomainDataSource.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("datasourceName")
                    .setType("odps")
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

    private final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithRaw =
            Flightdm.CommandDomainDataQuery.newBuilder()
                    .setContentType(Flightdm.ContentType.RAW)
                    .setPartitionSpec("partition_spec")
                    .build();

    private final Flightdm.CommandDomainDataUpdate commandDomainDataUpdateWithCsv =
            Flightdm.CommandDomainDataUpdate.newBuilder()
                    .setContentType(Flightdm.ContentType.CSV)
                    .setPartitionSpec("partition_spec")
                    .build();

    private final Flightdm.CommandDomainDataUpdate commandDomainDataUpdateWithRaw =
            Flightdm.CommandDomainDataUpdate.newBuilder()
                    .setContentType(Flightdm.ContentType.RAW)
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

        ScqlCommandJobConfig result = odpsParamConverter.convert(meshSqlQuery);
        assertNotNull(result);
        assertEquals(OdpsTypeEnum.SQL, result.getOdpsTypeEnum());
        assertEquals(testSql, result.taskRunSQL());

        this.testOdpsConnectConfig(result.getOdpsConnectConfig());
    }

    /**
     * Test Scenario: Test the convert method, set the input to CommandDataMeshQuery, and set the ContentType to RAW
     */
    @Test
    public void testConvertMeshQueryWithRawContentType() {

        when(meshQuery.getQuery()).thenReturn(commandDomainDataQueryWithRaw);
        when(meshQuery.getDomaindata()).thenReturn(domainData);
        when(meshQuery.getDatasource()).thenReturn(domainDataSource);

        OdpsTableQueryConfig result = this.testConvertMeshQueryWithNonType();
        assertEquals(OdpsTypeEnum.FILE, result.getOdpsTypeEnum());
    }


    /**
     * Test scenario: Test the convert method, enter the CommandDataMeshQuery type, and the ContentType to CSV
     */
    @Test
    public void testConvertMeshQueryWithCsvContentType() {
        when(meshQuery.getQuery()).thenReturn(commandDomainDataQueryWithCsv);
        when(meshQuery.getDomaindata()).thenReturn(domainData);
        when(meshQuery.getDatasource()).thenReturn(domainDataSource);

        OdpsTableQueryConfig result = this.testConvertMeshQueryWithNonType();
        assertEquals(OdpsTypeEnum.TABLE, result.getOdpsTypeEnum());
    }

    /**
     * Test Scenario: Test the convert method, enter the CommandDataMeshUpdate type, and set the ContentType to RAW
     */
    @Test
    public void testConvertMeshUpdateWithRawContentType() {

        when(meshUpdate.getUpdate()).thenReturn(commandDomainDataUpdateWithRaw);
        when(meshUpdate.getDomaindata()).thenReturn(domainData);
        when(meshUpdate.getDatasource()).thenReturn(domainDataSource);

        OdpsWriteConfig odpsWriteConfig = this.testConvertMeshUpdateWithNonType();
        assertEquals(OdpsTypeEnum.FILE, odpsWriteConfig.getOdpsTypeEnum());
    }

    /**
     * Test Scenario: Test the convert method, enter the CommandDataMeshUpdate type, and the ContentType to CSV
     */
    @Test
    public void testConvertMeshUpdateWithCsvContentType() {

        when(meshUpdate.getUpdate()).thenReturn(commandDomainDataUpdateWithCsv);
        when(meshUpdate.getDomaindata()).thenReturn(domainData);
        when(meshUpdate.getDatasource()).thenReturn(domainDataSource);

        OdpsWriteConfig odpsWriteConfig = this.testConvertMeshUpdateWithNonType();
        assertEquals(OdpsTypeEnum.TABLE, odpsWriteConfig.getOdpsTypeEnum());
    }

    private void testOdpsConnectConfig(OdpsConnectConfig odpsConnectConfig) {
        assertNotNull(odpsConnectConfig);
        assertEquals("accessKeyId", odpsConnectConfig.accessKeyId());
        assertEquals("accessKeySecret", odpsConnectConfig.accessKeySecret());
        assertEquals("project", odpsConnectConfig.projectName());
        assertEquals("endpoint", odpsConnectConfig.endpoint());
    }

    private OdpsTableQueryConfig testConvertMeshQueryWithNonType() {
        OdpsTableQueryConfig result = odpsParamConverter.convert(meshQuery);
        assertNotNull(result);

        this.testOdpsConnectConfig(result.getOdpsConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }

    private OdpsWriteConfig testConvertMeshUpdateWithNonType() {
        OdpsWriteConfig result = odpsParamConverter.convert(meshUpdate);
        assertNotNull(result);

        this.testOdpsConnectConfig(result.getOdpsConnectConfig());

        assertNotNull(result.getCommandConfig());
        assertEquals("table_name_or_file_path", result.getCommandConfig().tableName());
        assertEquals("partition_spec", result.getCommandConfig().partition());

        return result;
    }
}
