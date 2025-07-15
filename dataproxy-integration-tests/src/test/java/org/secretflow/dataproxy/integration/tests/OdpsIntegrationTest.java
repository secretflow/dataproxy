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
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.secretflow.dataproxy.common.utils.ArrowUtil;
import org.secretflow.dataproxy.integration.tests.utils.OdpsTestUtil;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author yuexie
 * @date 2025/2/28 14:41
 **/
@Slf4j
@EnabledIfSystemProperty(named = "enableIntegration", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OdpsIntegrationTest extends BaseArrowFlightServerTest {

    private final Domaindatasource.OdpsDataSourceInfo odpsDataSourceInfo =
            Domaindatasource.OdpsDataSourceInfo
                    .newBuilder()
                    .setAccessKeyId(OdpsTestUtil.getAccessKeyId())
                    .setAccessKeySecret(OdpsTestUtil.getAccessKeySecret())
                    .setProject(OdpsTestUtil.getOdpsProject())
                    .setEndpoint(OdpsTestUtil.getOdpsEndpoint())
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
                    .setRelativeUri("integration_test_table")
                    .setDomaindataId("domainDataId")
                    .setType("table")
                    .addAllColumns(columns)
                    .build();

    private final Domaindata.DomainData domainDataWithFile =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("domainDataName")
                    .setRelativeUri("integration_test_resource")
                    .setDomaindataId("domainDataId")
                    .setType("model")
                    .build();

    private final int batchSize = 100;
    private final int batchCount = 10;

    @TempDir
    private static Path tempDir;
    private static Path tmpFilePath;

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

    @Test
    @Order(2)
    public void testDoGetWithResource() {
        final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCsv =
                Flightdm.CommandDomainDataQuery.newBuilder()
                        .setContentType(Flightdm.ContentType.RAW)
                        .build();

        Flightinner.CommandDataMeshQuery query =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithFile)
                        .setQuery(commandDomainDataQueryWithCsv)
                        .build();

        this.testDoGet(query);
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

    @Test
    @Order(1)
    public void testCommandDataMeshUpdateWithFile() {
        Flightinner.CommandDataMeshUpdate commandDataMeshUpdate =
                Flightinner.CommandDataMeshUpdate.newBuilder()
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainDataWithFile)
                        .setUpdate(Flightdm.CommandDomainDataUpdate.newBuilder()
                                .setContentType(Flightdm.ContentType.RAW)
                                .build())
                        .build();
        this.testDoPut(commandDataMeshUpdate);
    }


    @Test
    @Order(2)
    public void testCommandDataSourceSqlQuery() {

        Flightinner.CommandDataMeshSqlQuery commandDataMeshSqlQuery =
                Flightinner.CommandDataMeshSqlQuery.newBuilder()
                        .setDatasource(domainDataSource)
                        .setQuery(Flightdm.CommandDataSourceSqlQuery.newBuilder()
                                .setSql("select * from integration_test_table limit 100;")
                                .setDatasourceId("datasourceId")
                                .build())
                        .build();

        this.testDoGetWithTable(commandDataMeshSqlQuery, 100);
    }

    private void testDoGet(final Flightinner.CommandDataMeshQuery query) {
        if (query.getQuery().getContentType() == Flightdm.ContentType.RAW) {
            testDoGetWithResource(query);
        } else {
            testDoGetWithTable(query, batchSize * batchCount);
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

    private void testDoGetWithResource(final Message msg) {
        assertDoesNotThrow(() -> {
                    FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
                    FlightInfo flightInfo = client.getInfo(descriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

                    assertFlightInfo(flightInfo);

                    Location location = flightInfo.getEndpoints().get(0).getLocations().get(0);

                    assertNotNull(tempDir);
                    Path downloadTmpFilePath = tempDir.resolve(String.format("odps-test-download-file-%d.dat", System.currentTimeMillis()));
                    Files.deleteIfExists(downloadTmpFilePath);


                    try (FlightClient flightEndpointClient = FlightClient.builder().location(location).allocator(allocator).build();
                         FlightStream stream = flightEndpointClient.getStream(flightInfo.getEndpoints().get(0).getTicket());
                         BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(downloadTmpFilePath.toFile()))
                    ) {
                        while (stream.next()) {

                            try (VectorSchemaRoot streamRoot = stream.getRoot()) {
                                for (FieldVector vector : streamRoot.getFieldVectors()) {
                                    assertInstanceOf(VarBinaryVector.class, vector);
                                    try (VarBinaryVector varBinaryVector = (VarBinaryVector) vector) {
                                        for (int i = 0; i < varBinaryVector.getValueCount(); i++) {
                                            byte[] object = varBinaryVector.getObject(i);
                                            bos.write(object);
                                        }
                                    }
                                }
                            }
                        }
                        bos.flush();
                    }
                    assertTrue(Files.exists(downloadTmpFilePath));
                    assertTrue(Files.size(downloadTmpFilePath) > 0);
                    assertEquals(getSha256(downloadTmpFilePath), getSha256(tmpFilePath));
                }
        );
    }

    private void testDoPut(final Flightinner.CommandDataMeshUpdate msg) {

        assertDoesNotThrow(() -> {

            FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
            FlightInfo flightInfo = client.getInfo(flightDescriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            assertFlightInfo(flightInfo);
            Ticket ticket = flightInfo.getEndpoints().get(0).getTicket();
            FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());

            if (msg.getUpdate().getContentType() == Flightdm.ContentType.RAW) {
                writeTestDataWithFile(descriptor);
            } else {
                writeTestDataWithTable(msg, descriptor);
            }
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

    private void writeTestDataWithFile(final FlightDescriptor descriptor) throws IOException {
        Schema schema = new Schema(List.of(
                new Field("binary_data", new FieldType(true, new ArrowType.Binary(), null), null)

        ));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            FlightClient.ClientStreamListener clientStreamListener = client.startPut(descriptor, root, new AsyncPutListener());

            assertNotNull(tempDir);
            tmpFilePath = tempDir.resolve("odps-test-file.dat");
            // 1MB
            // On the SDK side, you need to simulate files that exceed 128KB to verify the integrity of the files after batch transfer
            int chunkSize = 1024 * 1024;
            byte[] chunk = new byte[chunkSize];
            // Populate random data
            new Random().nextBytes(chunk);

            try (BufferedOutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(tmpFilePath))) {
                outputStream.write(chunk, 0, chunkSize);
            }

            try (BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(tmpFilePath.toFile()))) {
                int l;
                int i = 0;
                root.allocateNew();
                VarBinaryVector gender = (VarBinaryVector) root.getVector("binary_data");
                gender.allocateNew();
                byte[] bytes = new byte[128 * 1024];
                while ((l = fileInputStream.read(bytes)) != -1) {
                    gender.setSafe(i, bytes, 0, l);
                    i++;
                    gender.setValueCount(i);
                }
                root.setRowCount(i);
            }
            clientStreamListener.putNext();
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


    private static String getSha256(Path filePath) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (DigestInputStream dis = new DigestInputStream(new FileInputStream(filePath.toFile()), md)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }
        byte[] hashBytes = md.digest();
        return bytesToHex(hashBytes);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}