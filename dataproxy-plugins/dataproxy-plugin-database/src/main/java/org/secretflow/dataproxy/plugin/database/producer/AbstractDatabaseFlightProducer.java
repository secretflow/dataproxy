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

package org.secretflow.dataproxy.plugin.database.producer;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.secretflow.dataproxy.plugin.database.config.DatabaseCommandConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;
import org.secretflow.dataproxy.plugin.database.converter.DatabaseParamConverter;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseDoGetContext;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseReader;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.dataproxy.core.spi.producer.DataProxyFlightProducer;
import org.secretflow.dataproxy.plugin.database.utils.DaMengUtil;
import org.secretflow.dataproxy.plugin.database.utils.HiveUtil;
import org.secretflow.dataproxy.plugin.database.utils.OracleUtil;
import org.secretflow.dataproxy.plugin.database.writer.AbstractDatabaseRecordWriter;
import org.secretflow.dataproxy.plugin.database.writer.DamengRecordWriter;
import org.secretflow.dataproxy.plugin.database.writer.HiveRecordWriter;
import org.secretflow.dataproxy.plugin.database.writer.OracleRecordWriter;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public abstract class AbstractDatabaseFlightProducer extends NoOpFlightProducer implements DataProxyFlightProducer {
    private final TicketService ticketService = CacheTicketService.getInstance();
    protected String producerName;

    private static final Map<String, Function<DatabaseConnectConfig, Connection>> ProducerName2Init = new HashMap<>();
    static {
        ProducerName2Init.put("hive", HiveUtil::initHive);
        ProducerName2Init.put("oracle", OracleUtil::initOracle);
        ProducerName2Init.put("dameng", DaMengUtil::initDaMeng);
    }
    AbstractDatabaseFlightProducer() {
        this.setProducerName();
    }
    abstract void setProducerName();
    @Override
    public String getProducerName() {
        return producerName;
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        final Any any = GrpcUtils.parseOrThrow(descriptor.getCommand());
        try {
            boolean isPut = false;
            DatabaseCommandConfig<?> commandConfig = switch (any.getTypeUrl()) {
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshSqlQuery" ->
                        new DatabaseParamConverter().convert(any.unpack(Flightinner.CommandDataMeshSqlQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshQuery" ->
                        new DatabaseParamConverter().convert(any.unpack(Flightinner.CommandDataMeshQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshUpdate" -> {
                    isPut = true;
                    yield new DatabaseParamConverter().convert(any.unpack(Flightinner.CommandDataMeshUpdate.class));
                }
                default -> throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Unknown command type")
                        .toRuntimeException();
            };

            log.info("DatabaseFlightProducer#getFlightInfo, commandConfig: {}", JsonUtils.toString(commandConfig));

            byte[] bytes;

            List<FlightEndpoint> endpointList;
            if (isPut) {
                bytes = ticketService.generateTicket(ParamWrapper.of(getProducerName(), commandConfig));
                Flightdm.TicketDomainDataQuery ticketDomainDataQuery = Flightdm.TicketDomainDataQuery.newBuilder().setDomaindataHandle(new String(bytes)).build();
                bytes = Any.pack(ticketDomainDataQuery).toByteArray();
                endpointList = Collections.singletonList(
                        new FlightEndpoint(new Ticket(bytes), FlightServerContext.getInstance().getFlightServerConfig().getLocation())
                );
            } else {
                bytes = ticketService.generateTicket(ParamWrapper.of(getProducerName(), commandConfig));
                endpointList = Collections.singletonList(
                        new FlightEndpoint(new Ticket(bytes), FlightServerContext.getInstance().getFlightServerConfig().getLocation())
                );
            }
            // Only the protocol is used, and the concrete schema is not returned here.
            return new FlightInfo(DataProxyFlightProducer.DEFACT_SCHEMA, descriptor, endpointList, 0, 0,true, IpcOption.DEFAULT);
        } catch (InvalidProtocolBufferException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        } catch (Exception e) {
            log.error("getFlightInfo error", e);
            throw CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
        }
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        ParamWrapper paramWrapper = ticketService.getParamWrapper(ticket.getBytes());
        ArrowReader dbReader = null;

        try {
            Object param = paramWrapper.param();

            if (param == null) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The database read parameter is null");
            }

            if (param instanceof DatabaseCommandConfig<?> dbCommandConfig) {
                DatabaseDoGetContext dbDoGetContext = new DatabaseDoGetContext(dbCommandConfig, AbstractDatabaseFlightProducer.ProducerName2Init.get(this.producerName));

                List<TaskConfig> taskConfigs = dbDoGetContext.getTaskConfigs();
                dbReader = new DatabaseReader(new RootAllocator(), taskConfigs.get(0));
            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The database read parameter is invalid, type url: " + param.getClass());
            }
            listener.start(dbReader.getVectorSchemaRoot());
            while (true) {
                if (context.isCancelled()) {
                    log.warn("reader is cancelled");
                    break;
                }

                if (dbReader.loadNextBatch()) {
                    listener.putNext();
                } else{
                    break;
                }
            }
            listener.completed();
        } catch (InterruptedException | IOException e) {
            log.error("doGet error!", e);
            throw CallStatus.UNKNOWN
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        } finally {
            try {
                if (dbReader != null) {
                    dbReader.close();
                }
            } catch (Exception e) {
                log.error("close {} read error", this.producerName, e);
            }
        }

    }

    @Override
    public Runnable acceptPut(
            CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream
    ) {
        final Any any = GrpcUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

        if(!"type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.TicketDomainDataQuery".equals(any.getTypeUrl())) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The database write parameter is invalid, type url: " + any.getTypeUrl());
        }
        return () -> {
            try {
                Flightdm.TicketDomainDataQuery unpack = any.unpack(Flightdm.TicketDomainDataQuery.class);
                DatabaseWriteConfig writeConfig = ticketService.getParamWrapper(unpack.getDomaindataHandle().getBytes()).unwrap(DatabaseWriteConfig.class);

                AbstractDatabaseRecordWriter writer;
                int count = 0;
                writer = switch (this.producerName) {
                    case "hive" -> new HiveRecordWriter(writeConfig, ProducerName2Init.get(this.producerName));
                    case "oracle" -> new OracleRecordWriter(writeConfig, ProducerName2Init.get(this.producerName));
                    case "dameng" -> new DamengRecordWriter(writeConfig, ProducerName2Init.get(this.producerName));
                    default -> throw new IllegalStateException("Unexpected value: " + this.producerName);
                };

                String askMsg;
                VectorSchemaRoot vectorSchemaRoot;
                while (flightStream.next()) {
                    vectorSchemaRoot = flightStream.getRoot();
                    int rowCount = vectorSchemaRoot.getRowCount();
                    askMsg = "row count: " + rowCount;
                    writer.write(vectorSchemaRoot);

                    try (BufferAllocator ba = new RootAllocator(1024);
                         final ArrowBuf buffer = ba.buffer(askMsg.getBytes(StandardCharsets.UTF_8).length)) {
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                    count += rowCount;
                }
                ackStream.onCompleted();
                writer.close();
                log.info("put data over! all count: {}", count);
            } catch (InvalidProtocolBufferException e) {
                throw CallStatus.INVALID_ARGUMENT
                        .withCause(e)
                        .withDescription(e.getMessage())
                        .toRuntimeException();
            }
        };
    }
}
