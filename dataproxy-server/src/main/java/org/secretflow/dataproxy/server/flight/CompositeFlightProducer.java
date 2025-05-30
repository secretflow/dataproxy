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

package org.secretflow.dataproxy.server.flight;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.utils.GrpcUtils;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.service.TicketService;
import org.secretflow.dataproxy.core.service.impl.CacheTicketService;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;

/**
 * Composite flight producer.
 *
 * @author yuexie
 * @date 2024/10/30 16:33
 **/
@Slf4j
public class CompositeFlightProducer implements FlightProducer {

    private final ProducerRegistry registry;

    public CompositeFlightProducer(ProducerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Return data for a stream.
     *
     * @param context  Per-call context.
     * @param ticket   The application-defined ticket identifying this stream.
     * @param listener An interface for sending data back to the client.
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

        log.info("DoGet, ticket:{}", new String(ticket.getBytes(), StandardCharsets.UTF_8));
        try {
            this.getProducer(ticket).getStream(context, ticket, listener);
        } catch (Exception e) {
            log.error("doGet is happened error", e);
            throw CallStatus.INTERNAL
                    .withCause(e)
                    .withDescription(e.getMessage())
                    .toRuntimeException();
        }
    }

    /**
     * List available data streams on this service.
     *
     * @param context  Per-call context.
     * @param criteria Application-defined criteria for filtering streams.
     * @param listener An interface for sending data back to the client.
     */
    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        this.getProducer(criteria).listFlights(context, criteria, listener);
    }

    /**
     * Get information about a particular data stream.
     *
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {

        log.debug("getFlightInfo, descriptor:{}", descriptor);
        return this.getProducer(descriptor).getFlightInfo(context, descriptor);
    }


    /**
     * Begin or get an update on execution of a long-running query.
     *
     * <p>If the descriptor would begin a query, the server should return a response immediately to not
     * block the client. Otherwise, the server should not return an update until progress is made to
     * not spam the client with inactionable updates.
     *
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about execution.
     */
    @Override
    public PollInfo pollFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return this.getProducer(descriptor).pollFlightInfo(context, descriptor);
    }

    /**
     * Get schema for a particular data stream.
     *
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Schema for the stream.
     */
    @Override
    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
        try {
            return this.getProducer(descriptor).getSchema(context, descriptor);
        } catch (DataproxyException e) {
            log.error("[getStream] unknown DataproxyException", e);
            throw CallStatus.INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription(e.getDescription())
                    .toRuntimeException();
        } catch (Exception e) {
            log.error("[getStream] unknown exception", e);
            throw CallStatus.INTERNAL
                    .withCause(e)
                    .withDescription("Unknown exception")
                    .toRuntimeException();
        }
    }

    /**
     * Accept uploaded data for a particular stream.
     *
     * @param context      Per-call context.
     * @param flightStream The data stream being uploaded.
     * @param ackStream    A stream for sending acknowledgement messages.
     * @return A Runnable that will be called when the upload is complete.
     */
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        try {
            return this.getProducer(flightStream.getDescriptor()).acceptPut(context, flightStream, ackStream);
        } catch (Exception e) {
            log.error("Unknown exception", e);
            throw CallStatus.INTERNAL
                    .withCause(e)
                    .withDescription("Unknown exception:" + e.getMessage())
                    .toRuntimeException();
        }
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        this.getProducer(reader.getDescriptor()).doExchange(context, reader, writer);
    }

    /**
     * Generic handler for application-defined RPCs.
     *
     * @param context  Per-call context.
     * @param action   Client-supplied parameters.
     * @param listener A stream of responses.
     */
    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        this.getProducer("").doAction(context, action, listener);
    }

    /**
     * List available application-defined RPCs.
     *
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        this.getProducer("").listActions(context, listener);
    }

    private FlightProducer getProducer(String serviceName) {
        return registry.getOrDefaultNoOp(serviceName);
    }

    private FlightProducer getProducer(Ticket ticket) {

        log.debug("getProducer ticket: {}", ticket.getBytes());
        TicketService ticketService = CacheTicketService.getInstance();
        ParamWrapper paramWrapper = ticketService.getParamWrapper(ticket.getBytes());
        log.info("getProducer paramWrapper: {}", paramWrapper);

        if (paramWrapper != null) {
            return registry.getOrDefaultNoOp(paramWrapper.producerKey());
        } else {
            FlightProducer other = registry.getOrDefaultNoOp("other");
            log.info("getProducer other: {}", other);
            return other;
        }
    }

    private FlightProducer getProducer(Criteria criteria) {
        // no impl
        return registry.getOrDefaultNoOp(criteria.toString());
    }

    private FlightProducer getProducer(FlightDescriptor descriptor) {

        byte[] command = descriptor.getCommand();

        Any any = GrpcUtils.parseOrThrow(command);

        try {
            String dataSourceType = switch (any.getTypeUrl()) {
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshSqlQuery" ->
                        any.unpack(Flightinner.CommandDataMeshSqlQuery.class).getDatasource().getType();
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshQuery" ->
                        any.unpack(Flightinner.CommandDataMeshQuery.class).getDatasource().getType();
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.CommandDataMeshUpdate" ->
                        any.unpack(Flightinner.CommandDataMeshUpdate.class).getDatasource().getType();
                case "type.googleapis.com/kuscia.proto.api.v1alpha1.datamesh.TicketDomainDataQuery" -> {
                    String domaindataHandle = any.unpack(Flightdm.TicketDomainDataQuery.class).getDomaindataHandle();
                    yield CacheTicketService.getInstance().getParamWrapper(domaindataHandle.getBytes()).producerKey();
                }

                default -> throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Unknown command type")
                        .toRuntimeException();
            };
            log.info("producer type is {}", dataSourceType);
            return registry.getOrDefaultNoOp(dataSourceType);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
