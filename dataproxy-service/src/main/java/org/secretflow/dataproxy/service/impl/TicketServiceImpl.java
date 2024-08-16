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

package org.secretflow.dataproxy.service.impl;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.command.Command;
import org.secretflow.dataproxy.common.utils.IdUtils;
import org.secretflow.dataproxy.service.TicketService;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * ticket服务实现类
 *
 * @author muhong
 * @date 2023-08-31 11:50
 */
@Slf4j
@Service
public class TicketServiceImpl implements TicketService {

    /**
     * 超时时间
     */
    @Value("${dataproxy.ticket.timeout}")
    private int timeout = 300;

    /**
     * 是否一次性使用
     */
    @Value("${dataproxy.ticket.onlyOnce}")
    private boolean onlyOnce;

    private Cache<String, Command> ticketCache;

    @PostConstruct
    private void init() {
        // ticket暂时采用本地缓存方式实现
        ticketCache = Caffeine.newBuilder()
            .initialCapacity(5)
            .maximumSize(10)
            // 过期时间为5分钟
            .expireAfterWrite(timeout, TimeUnit.SECONDS)
            .build();
    }

    @Override
    public byte[] generateTicket(Command command) {
        String ticket = IdUtils.randomUUID();
        ticketCache.put(ticket, command);
        return ticket.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public synchronized Command getCommandByTicket(byte[] ticket) {
        String ticketStr = new String(ticket);

        Command command = ticketCache.getIfPresent(ticketStr);
        if (command == null) {
            throw DataproxyException.of(DataproxyErrorCode.TICKET_UNAVAILABLE);
        }

        if (onlyOnce) {
            // ticket只允许被消费一次
            ticketCache.invalidate(ticketStr);
        }
        return command;
    }
}
