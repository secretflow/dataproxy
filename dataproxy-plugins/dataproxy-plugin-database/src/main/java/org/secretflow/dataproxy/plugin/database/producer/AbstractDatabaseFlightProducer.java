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
import org.secretflow.dataproxy.plugin.database.config.DatabaseWriteConfig;
import org.secretflow.dataproxy.plugin.database.config.TaskConfig;
import org.secretflow.dataproxy.plugin.database.converter.DatabaseParamConverter;
import org.secretflow.dataproxy.plugin.database.reader.*;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.dataproxy.core.spi.producer.DataProxyFlightProducer;
import org.secretflow.dataproxy.plugin.database.writer.DatabaseRecordWriter;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Slf4j
public abstract class AbstractDatabaseFlightProducer extends NoOpFlightProducer implements DataProxyFlightProducer {
    private final TicketService ticketService = CacheTicketService.getInstance();

    @Override
    public abstract String  getProducerName();

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

    protected abstract DatabaseDoGetContext initDoGetContext(DatabaseCommandConfig<?> config);
    protected abstract DatabaseRecordWriter initRecordWriter(DatabaseWriteConfig config);
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        ParamWrapper paramWrapper = ticketService.getParamWrapper(ticket.getBytes());
        ArrowReader dbReader = null;
        DatabaseDoGetContext dbDoGetContext = null;
        try {
            Object param = paramWrapper.param();

            if (param == null) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The database read parameter is null");
            }

            if (param instanceof DatabaseCommandConfig<?> dbCommandConfig) {
                dbDoGetContext = initDoGetContext(dbCommandConfig);
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
                if (dbDoGetContext != null) {
                    dbDoGetContext.close();
                }
            } catch (Exception e) {
                log.error("close {} read error", this.getProducerName(), e);
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

                DatabaseRecordWriter writer;
                int count = 0;
                writer = initRecordWriter(writeConfig);

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
