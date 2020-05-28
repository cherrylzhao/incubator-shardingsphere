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

package org.apache.shardingsphere.database.protocol.postgresql.packet.generic;

import lombok.Getter;
import org.apache.shardingsphere.database.protocol.postgresql.packet.PostgreSQLPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.PostgreSQLCommandPacketType;
import org.apache.shardingsphere.database.protocol.postgresql.payload.PostgreSQLPacketPayload;

/**
 * Command complete packet for PostgreSQL.
 */
public final class PostgreSQLCommandCompletePacket implements PostgreSQLPacket {
    
    @Getter
    private final char messageType = PostgreSQLCommandPacketType.COMMAND_COMPLETE.getValue();
    
    private final String sqlCommand;
    
    private final long rows;
    
    private final long insertCount;
    
    public PostgreSQLCommandCompletePacket() {
        sqlCommand = "";
        rows = 0;
        insertCount = 0;
    }
    
    public PostgreSQLCommandCompletePacket(final String sqlCommand, final long rows, final long inertCount) {
        this.sqlCommand = sqlCommand;
        this.rows = rows;
        this.insertCount = inertCount;
    }
    
    @Override
    public void write(final PostgreSQLPacketPayload payload) {
        switch (sqlCommand) {
            case "INSERT":
                payload.writeStringNul(sqlCommand + " 0 1");
                break;
            case "UPDATE":
            case "DELETE":
            case "SELECT":
                payload.writeStringNul(sqlCommand + " " + rows);
                break;
            default:
                payload.writeStringNul(sqlCommand);
        }
    }
}
