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

package org.secretflow.dataproxy.server.flight;

import com.google.protobuf.Any;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.command.Command;
import org.secretflow.dataproxy.common.model.command.CommandTypeEnum;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.Dataset;
import org.secretflow.dataproxy.common.model.datasource.DatasetLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.Datasource;
import org.secretflow.dataproxy.common.model.datasource.location.OdpsTableInfo;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.common.utils.ProtoBufJsonUtils;
import org.secretflow.dataproxy.server.ProtoObjConvertor;
import org.secretflow.dataproxy.service.DataProxyService;
import org.secretflow.dataproxy.service.TicketService;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author muhong
 * @date 2023-09-13 16:08
 */
@Slf4j
@Service
public class DataproxyProducerImpl implements DataproxyProducer {

    private static final Schema DEFAULT_SCHEMA = new Schema(new ArrayList<>());
    @Autowired
    private TicketService ticketService;
    @Autowired
    private DataProxyService dataProxyService;
    @Autowired
    private Location location;
    @Autowired
    private BufferAllocator rootAllocator;

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        final Any command = GrpcUtils.parseOrThrow(descriptor.getCommand());

        try {
            if (command.is(Flightinner.CommandDataMeshQuery.class)) {
                return getFlightInfoQuery(GrpcUtils.unpackOrThrow(command, Flightinner.CommandDataMeshQuery.class), context, descriptor);
            } else if (command.is(Flightinner.CommandDataMeshUpdate.class)) {
                return getFlightInfoUpdate(GrpcUtils.unpackOrThrow(command, Flightinner.CommandDataMeshUpdate.class), context, descriptor);
            }
        } catch (DataproxyException e) {
            throw CallStatus.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getDescription())
                .toRuntimeException();
        } catch (Exception e) {
            throw CallStatus.INTERNAL
                .withCause(e)
                .withDescription("Unknown exception")
                .toRuntimeException();
        }

        log.error("[getFlightInfo] unrecognized request, type:{}", command.getTypeUrl());
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unrecognized request: " + command.getTypeUrl())
            .toRuntimeException();
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        final Any actionBody = GrpcUtils.parseOrThrow(action.getBody());

        Result result = null;
        try {

        } catch (DataproxyException e) {
            throw CallStatus.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getDescription())
                .toRuntimeException();
        } catch (Exception e) {
            throw CallStatus.INTERNAL
                .withCause(e)
                .withDescription("Unknown exception")
                .toRuntimeException();
        }

        if (result != null) {
            listener.onNext(result);
            listener.onCompleted();
            return;
        }

        log.error("[doAction] unrecognized request");
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unrecognized request: " + actionBody.getTypeUrl())
            .toRuntimeException();
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            getStreamReadData(context, ticket, listener);
        } catch (DataproxyException e) {
            throw CallStatus.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getDescription())
                .toRuntimeException();
        } catch (Exception e) {
            log.error("[getStream] unknown exception");
            throw CallStatus.INTERNAL
                .withCause(e)
                .withDescription("Unknown exception")
                .toRuntimeException();
        }
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        try {
            return acceptPutDataUpdate(context, flightStream, ackStream);
        } catch (DataproxyException e) {
            throw CallStatus.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getDescription())
                .toRuntimeException();
        } catch (Exception e) {
            log.error("[acceptPut] unknown exception");
            throw CallStatus.INTERNAL
                .withCause(e)
                .withDescription("Unknown exception")
                .toRuntimeException();
        }
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        // no implements
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public FlightInfo getFlightInfoQuery(Flightinner.CommandDataMeshQuery command, CallContext context, FlightDescriptor descriptor) {
        log.info("[getFlightInfoQuery] get flight info query start");

        try {
            Datasource datasource = ProtoObjConvertor.fromProto(command.getDatasource());
            Dataset dataset = ProtoObjConvertor.fromProto(command.getDomaindata(), datasource);

            // TODO: 不合理入参
            DatasetLocationConfig locationConfig = dataset.getLocationConfig();
            if (locationConfig.getLocationConfig() instanceof OdpsTableInfo odpsTableInfo) {
                String partitionSpec = command.getQuery().getPartitionSpec();
                locationConfig.setLocationConfig(new OdpsTableInfo(odpsTableInfo.tableName(), partitionSpec, odpsTableInfo.fields()));
            }

            Command readCommand = Command.builder()
                .type(CommandTypeEnum.READ)
                .commandInfo(DatasetReadCommand.builder()
                    .connConfig(datasource.getConnConfig())
                    .locationConfig(locationConfig)
                    .formatConfig(dataset.getFormatConfig())
                    .schema(dataset.getSchema().getArrowSchema())
                    .fieldList(command.getQuery().getColumnsList())
                    .outputFormatConfig(ProtoObjConvertor.fromProto(command.getQuery().getContentType()))
                    .build())
                .build();

            log.info("[getFlightInfoQuery] get flight info query, command:{}", JsonUtils.toJSONString(readCommand));

            byte[] ticketBytes = ticketService.generateTicket(readCommand);

            // 数据端，当前只支持1
            List<FlightEndpoint> endpointList = Collections.singletonList(
                new FlightEndpoint(new Ticket(ticketBytes), location));

            log.info("[getFlightInfoQuery] get flight info query completed");
            return new FlightInfo(DEFAULT_SCHEMA, descriptor, endpointList, 0, 0);
        } catch (DataproxyException e) {
            log.error("[getFlightInfoQuery] get flight info query error", e);
            throw e;
        } catch (Exception e) {
            log.error("[getFlightInfoQuery] get flight info query unknown exception", e);
            throw DataproxyException.of(DataproxyErrorCode.KUSCIA_GET_FLIGHT_INFO_QUERY_ERROR, e);
        }
    }

    @Override
    public FlightInfo getFlightInfoUpdate(Flightinner.CommandDataMeshUpdate command, CallContext context, FlightDescriptor descriptor) {
        log.info("[getFlightInfoUpdate] get flight info update start");

        try {
            Datasource datasource = ProtoObjConvertor.fromProto(command.getDatasource());
            Dataset dataset = ProtoObjConvertor.fromProto(command.getDomaindata(), datasource);

            // TODO: 不合理入参
            DatasetLocationConfig locationConfig = dataset.getLocationConfig();
            if (locationConfig.getLocationConfig() instanceof OdpsTableInfo odpsTableInfo) {
                String partitionSpec = command.getUpdate().getPartitionSpec();
                locationConfig.setLocationConfig(new OdpsTableInfo(odpsTableInfo.tableName(), partitionSpec, odpsTableInfo.fields()));
            }

            Command writeCommand = Command.builder()
                .type(CommandTypeEnum.WRITE)
                .commandInfo(DatasetWriteCommand.builder()
                    .connConfig(datasource.getConnConfig())
                    .locationConfig(locationConfig)
                    .formatConfig(dataset.getFormatConfig())
                    .schema(dataset.getSchema().getArrowSchema())
                    .inputFormatConfig(ProtoObjConvertor.fromProto(command.getUpdate().getContentType()))
                    .extraOptions(command.getUpdate().getExtraOptionsMap())
                    .build())
                .build();

            log.info("[getFlightInfoUpdate] get flight info update, command:{}", JsonUtils.toJSONString(writeCommand));

            byte[] ticketBytes = ticketService.generateTicket(writeCommand);
            Flightdm.TicketDomainDataQuery commandTicketWrite = Flightdm.TicketDomainDataQuery.newBuilder()
                .setDomaindataHandle(new String(ticketBytes))
                .build();

            // 数据端，当前只支持1
            List<FlightEndpoint> endpointList = Collections.singletonList(
                new FlightEndpoint(new Ticket(Any.pack(commandTicketWrite).toByteArray()), location));

            log.info("[getFlightInfoUpdate] get flight info update completed");
            return new FlightInfo(DEFAULT_SCHEMA, descriptor, endpointList, 0, 0);
        } catch (DataproxyException e) {
            log.error("[getFlightInfoUpdate] get flight info update error", e);
            throw e;
        } catch (Exception e) {
            log.error("[getFlightInfoUpdate] get flight info update unknown exception", e);
            throw DataproxyException.of(DataproxyErrorCode.KUSCIA_GET_FLIGHT_INFO_UPDATE_ERROR, e);
        }
    }

    public void getStreamReadData(CallContext context, Ticket ticket, ServerStreamListener listener) {
        log.info("[getStreamReadData] get stream start, ticket:{}", new String(ticket.getBytes()));

        try {
            // 根据ticket获取预先缓存的查询命令
            Command command = ticketService.getCommandByTicket(ticket.getBytes());
            if (command.getType() != CommandTypeEnum.READ) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "操作指令类型与接口不匹配");
            }

            log.info("[getStreamReadData] parse command from ticket success, command:{}", JsonUtils.toJSONString(command));
            try (ArrowReader arrowReader = dataProxyService.generateArrowReader(rootAllocator, (DatasetReadCommand) command.getCommandInfo())) {
                listener.start(arrowReader.getVectorSchemaRoot());
                while (arrowReader.loadNextBatch()) {
                    listener.putNext();
                }
                listener.completed();
                log.info("[getStreamReadData] get stream completed");
            }
        } catch (DataproxyException e) {
            log.error("[getStreamReadData] get stream error", e);
            throw e;
        } catch (Exception e) {
            log.error("[getStreamReadData] get stream unknown exception", e);
            throw DataproxyException.of(DataproxyErrorCode.KUSCIA_GET_STREAM_ERROR, e);
        }
    }

    public Runnable acceptPutDataUpdate(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        log.info("[acceptPutDataUpdate] accept put data (update) start");

        try {
            final Any acceptPutCommand = GrpcUtils.parseOrThrow(flightStream.getDescriptor().getCommand());
            if (!acceptPutCommand.is(Flightdm.TicketDomainDataQuery.class)) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "操作指令类型与接口不匹配");
            }

            Flightdm.TicketDomainDataQuery ticketDomainDataQuery = GrpcUtils.unpackOrThrow(acceptPutCommand, Flightdm.TicketDomainDataQuery.class);
            log.info("[acceptPutDataUpdate] parse ticketDomainDataQuery success, ticketDomainDataQuery:{}", ProtoBufJsonUtils.toJSONString(ticketDomainDataQuery));

            Command command = ticketService.getCommandByTicket(ticketDomainDataQuery.getDomaindataHandle().getBytes());
            log.info("[acceptPutDataUpdate] parse command from ticket success, command:{}", JsonUtils.toJSONString(command));
            if (command.getType() != CommandTypeEnum.WRITE) {
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "操作指令类型与接口不匹配");
            }

            return () -> {
                try {
                    dataProxyService.datasetWrite((DatasetWriteCommand) command.getCommandInfo(), flightStream,
                        root -> {
                            String msg = "row count: " + root.getRowCount();
                            try (final ArrowBuf buffer = rootAllocator.buffer(msg.getBytes(StandardCharsets.UTF_8).length)) {
                                buffer.writeBytes(msg.getBytes(StandardCharsets.UTF_8));
                                ackStream.onNext(PutResult.metadata(buffer));
                            }
                        });
                } catch (DataproxyException e) {
                    throw CallStatus.INTERNAL
                        .withCause(e)
                        .withDescription(e.getDescription())
                        .toRuntimeException();
                }
            };
        } catch (DataproxyException e) {
            log.error("[acceptPutDataUpdate] accept put data (update) error", e);
            throw e;
        } catch (Exception e) {
            log.error("[acceptPutDataUpdate] accept put data (update) unknown exception", e);
            throw DataproxyException.of(DataproxyErrorCode.KUSCIA_ACCEPT_PUT_ERROR, e);
        }
    }
}
