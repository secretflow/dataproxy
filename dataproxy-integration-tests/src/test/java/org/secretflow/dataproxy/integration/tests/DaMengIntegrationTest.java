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

package org.secretflow.dataproxy.integration.tests;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.secretflow.dataproxy.common.utils.ArrowUtil;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.integration.tests.utils.DamengTestUtil;
import org.secretflow.dataproxy.integration.tests.utils.HiveTestUtil;
import org.secretflow.dataproxy.server.DataProxyFlightServer;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@EnabledIfSystemProperty(named = "enableIntegration", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DaMengIntegrationTest extends BaseArrowFlightServerTest {

    private final Domaindatasource.DatabaseDataSourceInfo damengDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo
                    .newBuilder()
                    .setEndpoint(DamengTestUtil.getDamengEndpoint())
                    .setDatabase(DamengTestUtil.getDamengDatabase())
                    .setUser(DamengTestUtil.getUser())
                    .setPassword(DamengTestUtil.getPassword())
                    .build();
    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setDatabase(damengDataSourceInfo).build();

    private final Domaindatasource.DomainDataSource domainDataSource =
            Domaindatasource.DomainDataSource.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("datasourceName")
                    .setType("dameng")
                    .setInfo(dataSourceInfo)
                    .build();
    List<Common.DataColumn> columns = Arrays.asList(
            Common.DataColumn.newBuilder().setName("column_int").setType("int").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column_string").setType("string").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column_bool").setType("bool").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column_float").setType("float64").setComment("test table").build(),
            Common.DataColumn.newBuilder().setName("column_float32").setType("float32").setComment("test table").build()
    );

    private final Domaindata.DomainData domainDataWithTable =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("domainDataName")
                    .setRelativeUri("integrationtesttable")
                    .setDomaindataId("domainDataId")
                    .setType("table")
                    .addAllColumns(columns)
                    .build();

    private final int batchSize = 100;
    private final int batchCount = 10;

    @TempDir
    private static Path tempDir;
    private static Path tmpFilePath;

    @BeforeAll
    static public void startServer() {

        assertNotEquals("", DamengTestUtil.getDamengDatabase(), "dameng database is empty");
        assertNotEquals("", DamengTestUtil.getDamengEndpoint(), "daemng endpoint is empty");
        assertNotEquals("", DamengTestUtil.getUser(), "dameng user is empty");
        assertNotEquals("", DamengTestUtil.getPassword(), "dameng password is empty");

        dataProxyFlightServer = new DataProxyFlightServer(FlightServerContext.getInstance().getFlightServerConfig());

        assertDoesNotThrow(() -> {
            serverThread = new Thread(() -> {
                try {
                    dataProxyFlightServer.start();
                    SERVER_START_LATCH.countDown();
                    dataProxyFlightServer.awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    fail("Exception was thrown: " + e.getMessage());
                }
            });
        });

        assertDoesNotThrow(() -> {
            serverThread.start();
            SERVER_START_LATCH.await();
        });
    }

    @AfterAll
    static void stopServer() {
        assertDoesNotThrow(() -> {
            if (dataProxyFlightServer != null) dataProxyFlightServer.close();
            serverThread.interrupt();
        });
    }

    @Test
    @Order(2)
    public void testDoGetWithTable() {

        final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCsv =
                Flightdm.CommandDomainDataQuery.newBuilder()
                        .setContentType(Flightdm.ContentType.CSV)
                        .setPartitionSpec("")
                        .build();

        Flightinner.CommandDataMeshQuery query =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithTable)
                        .setQuery(commandDomainDataQueryWithCsv)
                        .build();

        this.testDoGet(query);
    }

    private void testDoGet(final Flightinner.CommandDataMeshQuery query) {
        testDoGetWithTable(query, batchSize * batchCount);

    }

    private void assertFlightInfo(FlightInfo flightInfo) {
        assertNotNull(flightInfo);
        assertNotNull(flightInfo.getEndpoints());
        assertFalse(flightInfo.getEndpoints().isEmpty());

        for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
            assertNotNull(endpoint);
            assertNotNull(endpoint.getTicket());
            assertNotNull(endpoint.getLocations());
            assertFalse(endpoint.getLocations().isEmpty());
            for (Location location : endpoint.getLocations()) {
                assertNotNull(location);
                assertNotNull(location.getUri());
                assertNotNull(location.getUri().getHost());
            }
        }
    }

    private void testDoGetWithTable(final Message msg, final long recordCount) {
        assertDoesNotThrow(() -> {
                    FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());

                    FlightInfo flightInfo = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

                    assertFlightInfo(flightInfo);

                    try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

                        long total = 0;
                        while (stream.next()) {
                            try (VectorSchemaRoot root = stream.getRoot()) {
                                assertNotNull(root);
                                assertNotNull(root.getSchema());
                                total += root.getRowCount();
                            }
                        }
                        assertEquals(recordCount, total);
                    }
                }
        );
    }

    @Test
    @Order(1)
    public void testCommandDataMeshUpdate() {
        Flightinner.CommandDataMeshUpdate commandDataMeshUpdate =
                Flightinner.CommandDataMeshUpdate.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithTable)
                        .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                                .setContentType(Flightdm.ContentType.CSV)
                                .setPartitionSpec("")
                                .build())
                        .build();
        this.testDoPut(commandDataMeshUpdate);
    }

    private void testDoPut(final Flightinner.CommandDataMeshUpdate msg) {

        assertDoesNotThrow(() -> {

            FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
            FlightInfo flightInfo = client.getInfo(flightDescriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            assertFlightInfo(flightInfo);
            Ticket ticket = flightInfo.getEndpoints().get(0).getTicket();
            FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());

            writeTestDataWithTable(msg, descriptor);

        });
    }

    private void writeTestDataWithTable(final Flightinner.CommandDataMeshUpdate msg, final FlightDescriptor descriptor) {

        assertNotNull(msg.getDomaindata());
        assertNotNull(msg.getDomaindata().getColumnsList());
        assertFalse(msg.getDomaindata().getColumnsList().isEmpty());

        Schema schema = new Schema(msg.getDomaindata().getColumnsList().stream()
                .map(column ->
                        Field.nullable(column.getName(), ArrowUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlightClient.ClientStreamListener clientStreamListener = client.startPut(descriptor, root, new AsyncPutListener());
            clientStreamListener.setUseZeroCopy(true);
            for (int i = 0; i < batchCount; i++) {
                writeTestData(root, batchSize);
                clientStreamListener.putNext();
            }
            clientStreamListener.completed();
            clientStreamListener.getResult();
        }
    }

    private void writeTestData(VectorSchemaRoot root, int rowCount) {

        Map<Class<? extends FieldVector>, BiConsumer<FieldVector, Integer>> strategyMap = new HashMap<>();

        strategyMap.put(VarCharVector.class, (fieldVector, index) ->
                ((VarCharVector) fieldVector).setSafe(index, ("test" + index).getBytes(StandardCharsets.UTF_8)));
        strategyMap.put(IntVector.class, (fieldVector, index) ->
                ((IntVector) fieldVector).setSafe(index, index));
        strategyMap.put(BigIntVector.class, (fieldVector, index) ->
                ((BigIntVector) fieldVector).setSafe(index, index));
        strategyMap.put(VarBinaryVector.class, (fieldVector, index) ->
                ((VarBinaryVector) fieldVector).setSafe(index, ("test" + index).getBytes(StandardCharsets.UTF_8)));
        strategyMap.put(Float4Vector.class, (fieldVector, index) ->
                ((Float4Vector) fieldVector).setSafe(index, index * 1.0f));
        strategyMap.put(Float8Vector.class, (fieldVector, index) ->
                ((Float8Vector) fieldVector).setSafe(index, index * 1.0d));
        strategyMap.put(BitVector.class, (fieldVector, index) ->
                ((BitVector) fieldVector).setSafe(index, index % 2 == 0 ? 1 : 0));
        strategyMap.put(DateDayVector.class, (fieldVector, index) ->
                ((DateDayVector) fieldVector).setSafe(index, index));
        strategyMap.put(DateMilliVector.class, (fieldVector, index) ->
                ((DateMilliVector) fieldVector).setSafe(index, index * 1000L));

        for (int i = 0; i < rowCount; i++) {
            for (FieldVector fieldVector : root.getFieldVectors()) {
                BiConsumer<FieldVector, Integer> biConsumer = strategyMap.get(fieldVector.getClass());
                assertNotNull(biConsumer);
                biConsumer.accept(fieldVector, i);
            }
        }
        root.setRowCount(rowCount);
    }
}
