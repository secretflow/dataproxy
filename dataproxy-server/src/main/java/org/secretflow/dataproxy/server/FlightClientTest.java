package org.secretflow.dataproxy.server;

import com.google.protobuf.Any;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.utils.ArrowUtil;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class FlightClientTest {
    private FlightClient client;
    protected BufferAllocator allocator;
    private final int batchSize = 5;
    private final int batchCount = 10;

    public FlightClientTest() {
        allocator = new RootAllocator(1024 * 1024 * 128);
        client = FlightClient.builder(allocator, FlightServerContext.getInstance().getFlightServerConfig().getLocation()).build();

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
//                assertNotNull(biConsumer);
                biConsumer.accept(fieldVector, i);
            }
        }
        root.setRowCount(rowCount);
    }
    private void writeTestDataWithTable(final Flightinner.CommandDataMeshUpdate msg, final FlightDescriptor descriptor) {

//        assertNotNull(msg.getDomaindata());
//        assertNotNull(msg.getDomaindata().getColumnsList());
//        assertFalse(msg.getDomaindata().getColumnsList().isEmpty());

        Schema schema = new Schema(msg.getDomaindata().getColumnsList().stream()
                .map(column ->
                        Field.nullable(column.getName(), ArrowUtil.parseKusciaColumnType(column.getType())))
                .collect(Collectors.toList()));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlightClient.ClientStreamListener clientStreamListener = client.startPut(descriptor, root, new AsyncPutListener());
            clientStreamListener.setUseZeroCopy(true);
            for (int i = 0; i < batchCount; i++) {
                log.info("1");
                writeTestData(root, batchSize);
                clientStreamListener.putNext();
            }
            clientStreamListener.completed();
            clientStreamListener.getResult();
        }

    }
    private final Domaindatasource.DatabaseDataSourceInfo hiveDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo
                    .newBuilder()
                    .setDatabase("dataproxy_alice")
                    .setEndpoint("127.0.0.1:10000")
                    .setUser("")
                    .setPassword("")
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
        log.info("exit ");
    }
    private void testDoPut(final Flightinner.CommandDataMeshUpdate msg) {
            FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(msg).toByteArray());
            FlightInfo flightInfo = client.getInfo(flightDescriptor, CallOptions.timeout(10, TimeUnit.SECONDS));

            Ticket ticket = flightInfo.getEndpoints().get(0).getTicket();
            FlightDescriptor descriptor = FlightDescriptor.command(ticket.getBytes());
            writeTestDataWithTable(msg, descriptor);
    }


}
