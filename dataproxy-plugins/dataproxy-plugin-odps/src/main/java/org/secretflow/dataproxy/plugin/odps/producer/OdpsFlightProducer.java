/*
 * Copyright 2024 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.odps.producer;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.dataproxy.core.spi.producer.DataProxyFlightProducer;
import org.secretflow.dataproxy.core.writer.Writer;
import org.secretflow.dataproxy.plugin.odps.config.OdpsCommandConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsTableConfig;
import org.secretflow.dataproxy.plugin.odps.config.OdpsWriteConfig;
import org.secretflow.dataproxy.plugin.odps.config.TaskConfig;
import org.secretflow.dataproxy.plugin.odps.constant.OdpsTypeEnum;
import org.secretflow.dataproxy.plugin.odps.converter.OdpsParamConverter;
import org.secretflow.dataproxy.plugin.odps.reader.OdpsDoGetContext;
import org.secretflow.dataproxy.plugin.odps.reader.OdpsReader;
import org.secretflow.dataproxy.plugin.odps.reader.OdpsResourceReader;
import org.secretflow.dataproxy.plugin.odps.writer.OdpsRecordWriter;
import org.secretflow.dataproxy.plugin.odps.writer.OdpsResourceWriter;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * @author yuexie
 * @date 2024/11/7 16:15
 **/
@Slf4j
public class OdpsFlightProducer extends NoOpFlightProducer implements DataProxyFlightProducer {

    private final TicketService ticketService = CacheTicketService.getInstance();
    /**
     * Obtain the data type used for registration name and identification processing.
     *
     * @return producer name
     */
    @Override
    public String getProducerName() {
        return "odps";
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {

        final Any any = GrpcUtils.parseOrThrow(descriptor.getCommand());

        try {
            boolean isPut = false;
            OdpsCommandConfig<?> commandConfig = switch (any.getTypeUrl()) {
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshSqlQuery" ->
                        new OdpsParamConverter().convert(any.unpack(Flightinner.CommandDataMeshSqlQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshQuery" ->
                        new OdpsParamConverter().convert(any.unpack(Flightinner.CommandDataMeshQuery.class));
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshUpdate" -> {
                    isPut = true;
                    yield new OdpsParamConverter().convert(any.unpack(Flightinner.CommandDataMeshUpdate.class));
                }
                default -> throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Unknown command type")
                        .toRuntimeException();
            };

            log.info("OdpsFlightProducer#getFlightInfo, commandConfig: {}", JsonUtils.toString(commandConfig));

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
        ArrowReader odpsReader = null;
        try {

            Object param = paramWrapper.param();

            if (param == null) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The odps read parameter is null");
            }

            if (param instanceof OdpsCommandConfig<?> odpsCommandConfig) {
                if (OdpsTypeEnum.FILE.equals(odpsCommandConfig.getOdpsTypeEnum())) {
                    Object commandConfig = odpsCommandConfig.getCommandConfig();
                    if (commandConfig instanceof OdpsTableConfig odpsTableConfig) {
                        odpsReader = new OdpsResourceReader(new RootAllocator(), odpsCommandConfig.getOdpsConnectConfig(), odpsTableConfig);
                    } else {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The odps read parameter is invalid, type url: " + commandConfig.getClass());
                    }
                } else {
                    OdpsDoGetContext odpsDoGetContext = new OdpsDoGetContext(odpsCommandConfig, false);
                    List<TaskConfig> taskConfigs = odpsDoGetContext.getTaskConfigs();

                    if (taskConfigs.isEmpty()) {
                        throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The odps read parameter is invalid, taskConfigs size is 0");
                    }

                    odpsReader = new OdpsReader(new RootAllocator(), taskConfigs.get(0));
                }
            } else if (param instanceof TaskConfig taskConfig) {
                odpsReader = new OdpsReader(new RootAllocator(), taskConfig);
            } else {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The odps read parameter is invalid, type url: " + param.getClass());
            }

            listener.start(odpsReader.getVectorSchemaRoot());
            while (true) {
                if (context.isCancelled()) {
                    log.warn("reader is cancelled");
                    break;
                }

                if (odpsReader.loadNextBatch()) {
                    listener.putNext();
                } else {
                    break;
                }
            }
            log.info("doGet is completed");
            listener.completed();
        } catch (Exception e) {
            log.error("doGet error", e);
            throw CallStatus.UNKNOWN
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        } finally {
            try {
                if (odpsReader != null) {
                    odpsReader.close();
                }
            } catch (Exception e) {
                log.error("close odps reader error", e);
            }
        }
    }

    @Override
    public Runnable acceptPut(
            CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {

        final Any any = GrpcUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

        if (!"type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.TicketDomainDataQuery".equals(any.getTypeUrl())) {
            throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "The odps write parameter is invalid, type url: " + any.getTypeUrl());
        }

        return () -> {
            try {
                Flightdm.TicketDomainDataQuery unpack = any.unpack(Flightdm.TicketDomainDataQuery.class);
                OdpsWriteConfig writeConfig = ticketService.getParamWrapper(unpack.getDomaindataHandle().getBytes()).unwrap(OdpsWriteConfig.class);

                Writer writer;
                if (OdpsTypeEnum.FILE.equals(writeConfig.getOdpsTypeEnum())) {
                    writer = new OdpsResourceWriter(writeConfig.getOdpsConnectConfig(), writeConfig.getCommandConfig());
                } else {
                    writer = new OdpsRecordWriter(writeConfig);
                }

                VectorSchemaRoot vectorSchemaRoot;
                String askMsg;
                int count = 0;
                while (flightStream.next()) {
                    vectorSchemaRoot = flightStream.getRoot();
                    int rowCount = vectorSchemaRoot.getRowCount();
                    askMsg = "row count: " + rowCount;
                    writer.write(vectorSchemaRoot);

                    try(BufferAllocator ba = new RootAllocator(1024);
                        final ArrowBuf buffer = ba.buffer(askMsg.getBytes(StandardCharsets.UTF_8).length)) {
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                    count += rowCount;
                }
                writer.flush();
                ackStream.onCompleted();
                log.info("put data over! all count: {}", count);
            } catch (InvalidProtocolBufferException e) {
                throw CallStatus.INVALID_ARGUMENT
                        .withCause(e)
                        .withDescription(e.getMessage())
                        .toRuntimeException();
            } catch (Exception e) {
                log.error("unknown error", e);
                throw CallStatus.INTERNAL
                        .withCause(e)
                        .withDescription(e.getMessage())
                        .toRuntimeException();
            }
        };
    }
}
