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

import org.secretflow.dataproxy.common.utils.GrpcUtils;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * 用于连接fastDS的flightServer，使用对应数据服务
 *
 * @author yumu
 * @date 2023/8/16 17:23
 */
@Slf4j
public class DPFlightClient {

    private FlightClient flightClient;


    /**
     * 构造函数，构建对应的client连接server
     *
     * @param location 用于定位flightServer服务的位置，重点在uri
     */
    public DPFlightClient(BufferAllocator allocator, Location location) {
        this.flightClient = FlightClient.builder()
            .allocator(allocator)
            .location(location)
            .build();
    }

    public FlightInfo getFlightInfo(Message readCmd) {
        FlightDescriptor flightDescriptor = FlightDescriptor.command(Any.pack(readCmd).toByteArray());
        return flightClient.getInfo(flightDescriptor);
    }


    /**
     * 从server端消费数据，并将下载得到的数据存储至指定位置
     *
     * @param flightInfo 唯一定位server端的某组数据，包含schema，下载endpoints等信息
     */
    public void downloadStructDataAndPrint(FlightInfo flightInfo) {
        // 1、获取endpoint列表及其ticket信息
        List<FlightEndpoint> endpointList = flightInfo.getEndpoints();

        // 2、针对每一个endpoint，执行数据下载操作
        for (FlightEndpoint endpoint : endpointList) {
            try (FlightStream flightStream = flightClient.getStream(endpoint.getTicket())) {
                VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot();
                while (flightStream.next()) {
                    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                         ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRootReceived, null, Channels.newChannel(out))) {
                        writer.start();
                        writer.writeBatch();
                        System.out.println(vectorSchemaRootReceived.contentToTSVString());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void uploadAllTypeData(FlightInfo flightInfo, VectorSchemaRoot root) {
        final Any acceptPutCommand = GrpcUtils.parseOrThrow(flightInfo.getEndpoints().get(0).getTicket().getBytes());
        Flightdm.TicketDomainDataQuery ticketDomainDataQuery = GrpcUtils.unpackOrThrow(acceptPutCommand, Flightdm.TicketDomainDataQuery.class);

        FlightClient.ClientStreamListener listener = flightClient.startPut(
            FlightDescriptor.command(Any.pack(ticketDomainDataQuery).toByteArray()), root, new AsyncPutListener());
        for (int i = 1; i < 10; i++) {
            listener.putNext();
        }
        listener.completed();
        listener.getResult();
    }
}
