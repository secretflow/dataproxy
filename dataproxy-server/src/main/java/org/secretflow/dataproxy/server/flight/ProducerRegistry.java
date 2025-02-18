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

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.NoOpFlightProducer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aggregation FlightProducer registry.
 *
 * @author yuexie
 * @date 2024/10/30 16:35
 **/
public final class ProducerRegistry {

    private static final ProducerRegistry INSTANCE = new ProducerRegistry();

    private final Map<String, FlightProducer> producers = new ConcurrentHashMap<>(8);

    private final NoOpFlightProducer noOpFlightProducer = new NoOpFlightProducer();

    private ProducerRegistry() {
        // Private constructor to prevent instantiation from outside
    }

    public static ProducerRegistry getInstance() {
        return INSTANCE;
    }

    public void register(String key, FlightProducer service) {
        // Implementation to register a service
        producers.put(key, service);
    }

    public FlightProducer getOrDefaultNoOp(String key) {
        // Implementation to retrieve a service by name
        return producers.getOrDefault(key, noOpFlightProducer);
    }
}
