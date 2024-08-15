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

package org.secretflow.dataproxy.server.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;

/**
 * @author muhong
 * @date 2023-08-07 10:43 AM
 */
@Slf4j
@Configuration
public class ArrowConfig {

    @Value("${dataproxy.flight.port}")
    private int defaultPort;

    @Bean
    public BufferAllocator bufferAllocator() {
        return new RootAllocator();
    }

    @Bean
    public Location location() {
        try {
            String localMachineHost = "";
            final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                    continue;
                }
                final Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress inetAddress = addresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress instanceof Inet4Address) {
                        localMachineHost = inetAddress.getHostAddress();
                    }
                }
            }

            int port = parsePort();
            return Location.forGrpcInsecure(localMachineHost, port);
        } catch (Exception e) {
            log.error("config location error", e);
            throw new RuntimeException(e);
        }
    }

    private int parsePort() {
        String dpConfigFile = System.getenv("DP_CONFIG_FILE");
        if (StringUtils.isEmpty(dpConfigFile)) {
            log.info("dp config file env not found, use default port");
            return defaultPort;
        }

        String dpConfigJson = null;
        try {
            dpConfigJson = Files.readString(Paths.get(dpConfigFile), Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("dp config file read error", e);
        }

        DPConfig dpConfig = JsonUtils.toJavaObject(dpConfigJson, DPConfig.class);
        AllocatedPorts allocatedPorts = JsonUtils.toJavaObject(dpConfig.getAllocated_ports(), AllocatedPorts.class);
        for (AllocatedPort arrowFlightPort : allocatedPorts.getPorts()) {
            if (arrowFlightPort.getName().equals("dp")) {
                return arrowFlightPort.getPort();
            }
        }
        throw new RuntimeException("dp port config not found in " + dpConfigFile);
    }

    @Data
    private static class DPConfig {
        private String allocated_ports;
    }

    @Data
    private static class AllocatedPorts {
        private List<AllocatedPort> ports;
    }

    @Data
    private static class AllocatedPort {
        private String name;
        private Integer port;
    }
}
