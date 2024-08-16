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

package org.secretflow.dataproxy.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

/**
 * arrow flight服务启动类
 *
 * @author muhong
 * @date 2023-08-07 10:26 AM
 */
@Slf4j
@Service
public class DataproxyLauncher implements CommandLineRunner {

    private FlightServer flightServer;

    @Autowired
    private BufferAllocator bufferAllocator;

    @Autowired
    private FlightProducer flightProducer;

    @Autowired
    private Location location;

    /**
     * GRPC 服务启动方法
     */
    private void grpcStart() {
        FlightServer.Builder flightServerBuilder = FlightServer.builder()
            .allocator(bufferAllocator)
            .middleware(FlightServerTraceMiddleware.getKey(), new FlightServerTraceMiddleware.FlightServerTraceMiddlewareFactory())
            .location(location);

        try (FlightServer server = flightServerBuilder.producer(flightProducer).build()) {
            flightServer = server;
            flightServer.start();
            log.info("Fastds server launch success, listening on port {}， ip:{}", flightServer.getPort(), location.getUri().getHost());
            flightServer.awaitTermination();
            Runtime.getRuntime().addShutdownHook(new Thread(this::grpcStop));
        } catch (Exception e) {
            log.error("fastds launch failed", e);
            throw new RuntimeException("Failed to start Flight Server", e);
        }
    }

    /**
     * GRPC 服务Stop方法
     */
    private void grpcStop() {
        if (flightServer != null) {
            try {
                flightServer.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run(String... args) throws Exception {
        grpcStart();
    }
}
