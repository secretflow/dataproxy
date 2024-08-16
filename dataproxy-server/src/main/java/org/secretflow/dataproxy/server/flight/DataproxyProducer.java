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

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

/**
 * Dataproxy facade
 *
 * @author muhong
 * @date 2023-09-13 16:05
 */
public interface DataproxyProducer extends FlightProducer, AutoCloseable {

    /**
     * (Query) get flight info
     *
     * @param command    Read command
     * @param context    Context
     * @param descriptor Raw descriptor
     * @return
     */
    FlightInfo getFlightInfoQuery(Flightinner.CommandDataMeshQuery command, CallContext context, FlightDescriptor descriptor);

    /**
     * (Update) get flight info
     *
     * @param command    Update command
     * @param context    Context
     * @param descriptor Raw descriptor
     * @return
     */
    FlightInfo getFlightInfoUpdate(Flightinner.CommandDataMeshUpdate command, CallContext context, FlightDescriptor descriptor);
}