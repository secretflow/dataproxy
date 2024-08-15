/*
 * Copyright 2023 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.integration.tests;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.secretflow.dataproxy.integration.tests.config.LocalHostKusciaConnectorConfig;
import org.secretflow.dataproxy.integration.tests.config.OssKusciaConnectorConfig;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.util.Arrays;
import java.util.List;

/**
 * @author muhong
 * @date 2023-11-17 11:07
 */
public class DataproxyKusciaTest {
    private final static String DP_HOST = "";
    private final static int DP_PORT = 8023;

    private final static BufferAllocator allocator = new RootAllocator();

    private final static List<KusciaConnectorConfig> configList = Arrays.asList(
//        new MysqlKusciaConnectorConfig(),
        new OssKusciaConnectorConfig(),
        new LocalHostKusciaConnectorConfig()
    );

    public static void main(String[] args) {
        for (KusciaConnectorConfig connectorConfig : configList) {
            writeTest(connectorConfig.getDatasource(), connectorConfig.getDataset());
            readTest(connectorConfig.getDatasource(), connectorConfig.getDataset());
        }
    }

    public static void writeTest(Domaindatasource.DomainDataSource dataSource, Domaindata.DomainData domainData) {
        Flightdm.CommandDomainDataUpdate updateCommand = Flightdm.CommandDomainDataUpdate.newBuilder()
            .setDomaindataId(domainData.getDomaindataId())
            .setContentType(Flightdm.ContentType.Table)
            .build();

        Flightinner.CommandDataMeshUpdate update = Flightinner.CommandDataMeshUpdate.newBuilder()
            .setUpdate(updateCommand)
            .setDatasource(dataSource)
            .setDomaindata(domainData)
            .build();

        FlightInfo flightInfo = getFlightClient().getFlightInfo(update);
        getFlightClientFromFlightInfo(flightInfo).uploadAllTypeData(flightInfo, TestDataUtils.generateKusciaAllTypeData(allocator));
    }

    public static void readTest(Domaindatasource.DomainDataSource dataSource, Domaindata.DomainData domainData) {
        Flightinner.CommandDataMeshQuery query = Flightinner.CommandDataMeshQuery.newBuilder()
            .setQuery(Flightdm.CommandDomainDataQuery.newBuilder()
                .setDomaindataId(domainData.getDomaindataId())
                .setContentType(Flightdm.ContentType.Table)
                .build())
            .setDatasource(dataSource)
            .setDomaindata(domainData)
            .build();

        FlightInfo flightInfo = getFlightClient().getFlightInfo(query);
        getFlightClientFromFlightInfo(flightInfo).downloadStructDataAndPrint(flightInfo);
    }

    private static DPFlightClient getFlightClient() {
        return new DPFlightClient(allocator, Location.forGrpcInsecure(DP_HOST, DP_PORT));
    }

    private static DPFlightClient getFlightClientFromFlightInfo(FlightInfo flightInfo) {
        Location location = flightInfo.getEndpoints().get(0).getLocations().get(0);
        return new DPFlightClient(allocator, location);
    }
}
