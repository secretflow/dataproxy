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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.secretflow.dataproxy.core.config.FlightServerContext;
import org.secretflow.dataproxy.integration.tests.utils.OdpsTestUtil;
import org.secretflow.dataproxy.server.DataProxyFlightServer;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author yuexie
 * @date 2025/2/7 13:56
 **/
public class BaseArrowFlightServerTest {

    private static DataProxyFlightServer dataProxyFlightServer;
    protected FlightClient client;
    protected BufferAllocator allocator;
    private static Thread serverThread;
    private static final CountDownLatch SERVER_START_LATCH = new CountDownLatch(1);

    @BeforeAll
    public static void startServer() {

        assertNotEquals("", OdpsTestUtil.getOdpsProject(), "odps project is empty");
        assertNotEquals("", OdpsTestUtil.getOdpsEndpoint(), "odps endpoint is empty");
        assertNotEquals("", OdpsTestUtil.getAccessKeyId(), "odps access key id is empty");
        assertNotEquals("", OdpsTestUtil.getAccessKeySecret(), "odps access key secret is empty");

        dataProxyFlightServer = new DataProxyFlightServer(FlightServerContext.getInstance().getFlightServerConfig());

        assertDoesNotThrow(() -> {
            serverThread = new Thread(() -> {
                try {
                    dataProxyFlightServer.start();
                    SERVER_START_LATCH.countDown();
                    dataProxyFlightServer.awaitTermination();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    fail("Exception was thrown: " + e.getMessage());
                }
            });
        });

        assertDoesNotThrow(() -> {
            serverThread.start();
            SERVER_START_LATCH.await();
        });
    }

    @AfterAll
    static void stopServer() {
        assertDoesNotThrow(() -> {
            if (dataProxyFlightServer != null) dataProxyFlightServer.close();
            serverThread.interrupt();
        });
    }

    @BeforeEach
    public void setUp() {
        try {
            allocator = new RootAllocator(1024 * 1024 * 128);
            client = FlightClient.builder(allocator, FlightServerContext.getInstance().getFlightServerConfig().getLocation()).build();
        } catch (Exception e) {
            fail("Exception was thrown: " + e.getMessage());
        }

    }

    @AfterEach
    public void tearDown() {
        AutoCloseable closeable = () -> {
            client.close();
            allocator.close();
        };
        assertDoesNotThrow(closeable::close);
    }
}
