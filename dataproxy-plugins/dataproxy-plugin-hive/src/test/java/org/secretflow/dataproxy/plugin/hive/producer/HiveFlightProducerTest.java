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

package org.secretflow.dataproxy.plugin.hive.producer;

import com.google.protobuf.Any;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveFlightProducerTest {
    @InjectMocks
    private HiveFlightProducer hiveFlightProducer;

    @Mock
    private FlightProducer.CallContext context;

    @Mock
    private FlightDescriptor descriptor;

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

    private final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCSV =
            Flightdm.CommandDomainDataQuery.newBuilder()
                    .setContentType(Flightdm.ContentType.CSV)
                    .setPartitionSpec("partition_spec")
                    .build();

    @Test
    public void testGetProducerName() {
        String producerName = hiveFlightProducer.getProducerName();
        assertEquals("hive", producerName);
    }

    @Test
    public void testGetFlightInfoWithTableCommand() {
        Flightinner.CommandDataMeshQuery dataMeshQuery =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setQuery(commandDomainDataQueryWithCSV)
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainData)
                        .build();
        when(descriptor.getCommand()).thenReturn(Any.pack(dataMeshQuery).toByteArray());
        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = hiveFlightProducer.getFlightInfo(context, descriptor);

            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());

            assertNotNull(flightInfo.getEndpoints().get(0).getLocations());
            assertFalse(flightInfo.getEndpoints().get(0).getLocations().isEmpty());

            assertNotNull(flightInfo.getEndpoints().get(0).getTicket());
            assertNotNull(flightInfo.getEndpoints().get(0).getTicket().getBytes());

        });
    }

    @Test
    public void testGetFlightInfoWithUnsupportedType() {
        when(descriptor.getCommand()).thenReturn("testCommand".getBytes(StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> hiveFlightProducer.getFlightInfo(context, descriptor));
    }
}
