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

package org.secretflow.dataproxy.server.flight.middleware;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.secretflow.dataproxy.common.utils.IdUtils;
import org.slf4j.MDC;

import java.util.Objects;

/**
 * trace_id - oriented middleware
 *
 * @author muhong
 * @date 2023-09-25 14:39
 */
public class FlightServerTraceMiddleware implements FlightServerMiddleware {

    public static final String TRACE_ID_KEY = "trace-id";
    private static final String GENERATE_TRACE_ID_PREFIX = "DP-FLIGHT";

    public static Key<FlightServerTraceMiddleware> getKey() {
        return FlightServerMiddleware.Key.of(FlightServerTraceMiddleware.class.getCanonicalName());
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {

    }

    @Override
    public void onCallCompleted(CallStatus status) {
        MDC.remove("TraceId");
    }

    @Override
    public void onCallErrored(Throwable err) {
        MDC.remove("TraceId");
    }

    public static class FlightServerTraceMiddlewareFactory implements Factory<FlightServerTraceMiddleware> {
        @Override
        public FlightServerTraceMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
            // Set the call link: Trace ID
            String traceId = incomingHeaders.get(TRACE_ID_KEY);
            // If no trace id is passed, one is generated
            if (Objects.isNull(traceId) || traceId.isEmpty()) {
                traceId = GENERATE_TRACE_ID_PREFIX + "-" + IdUtils.createRandString(32);
            }
            MDC.put("TraceId", traceId);
            return new FlightServerTraceMiddleware();
        }
    }
}
