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

package org.secretflow.dataproxy.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.secretflow.dataproxy.core.config.FlightServerConfig;
import org.secretflow.dataproxy.core.spi.producer.DataProxyFlightProducer;
import org.secretflow.dataproxy.server.flight.CompositeFlightProducer;
import org.secretflow.dataproxy.server.flight.ProducerRegistry;
import org.secretflow.dataproxy.server.flight.middleware.FlightServerTraceMiddleware;

import java.io.IOException;
import java.util.ServiceLoader;

/**
 * Data Proxy server main class.
 *
 * @author yuexie
 * @date 2024/10/30 16:03
 **/
@Slf4j
public class DataProxyFlightServer implements AutoCloseable {

    private final FlightServer server;

    public DataProxyFlightServer(FlightServerConfig config) {
        server = init(config);
    }

    public void start() throws IOException {
        server.start();
        log.info(server.getLocation().getUri().getHost());
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() throws Exception {
        server.close();
    }

    private FlightServer init(FlightServerConfig config) {

        BufferAllocator allocator = new RootAllocator();

        return FlightServer.builder()
//                .useTls(null, null)
//                .useMTlsClientVerification(null)
                .middleware(FlightServerTraceMiddleware.getKey(), new FlightServerTraceMiddleware.FlightServerTraceMiddlewareFactory())
                .allocator(allocator)
                .location(config.getLocation())
                .producer(initProducer())
                .build();
    }

    private CompositeFlightProducer initProducer() {

        ServiceLoader<DataProxyFlightProducer> serviceLoader = ServiceLoader.load(DataProxyFlightProducer.class, DataProxyFlightProducer.class.getClassLoader());

        ProducerRegistry registry = ProducerRegistry.getInstance();
        for (DataProxyFlightProducer producer : serviceLoader) {
            registry.register(producer.getProducerName(), producer);
            log.info("ProducerRegistry register: {}", producer.getProducerName());
        }

        return new CompositeFlightProducer(registry);
    }

}
