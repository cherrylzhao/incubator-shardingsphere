/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingproxy.frontend.postgresql.command.query.binary.sync;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLReadyForQueryPacket;
import org.apache.shardingsphere.shardingproxy.backend.communication.jdbc.connection.BackendConnection;
import org.apache.shardingsphere.shardingproxy.frontend.api.CommandExecutor;
import org.apache.shardingsphere.shardingproxy.frontend.api.SyncExecutor;

import java.util.Collection;
import java.util.Collections;

/**
 * Command sync executor for PostgreSQL.
 */
@RequiredArgsConstructor
public final class PostgreSQLComSyncExecutor implements CommandExecutor, SyncExecutor {
    
    private final BackendConnection backendConnection;
    
    @Override
    public Collection<DatabasePacket> execute() {
        return Collections.singletonList(new PostgreSQLReadyForQueryPacket(backendConnection.getStateHandler().isInTransaction()));
    }
}
