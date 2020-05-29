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

package org.apache.shardingsphere.shardingproxy.frontend.postgresql.command.query.binary.bind;

import org.apache.shardingsphere.database.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.database.protocol.postgresql.constant.PostgreSQLColumnType;
import org.apache.shardingsphere.database.protocol.postgresql.packet.PostgreSQLPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLColumnDescription;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLRowDescriptionPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.binary.bind.PostgreSQLBinaryResultSetRowPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.binary.bind.PostgreSQLBindCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.binary.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.text.PostgreSQLDataRowPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLCommandCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLErrorResponsePacket;
import org.apache.shardingsphere.shardingproxy.backend.binary.BinaryProtocolBackendHandlerFactory;
import org.apache.shardingsphere.shardingproxy.backend.communication.jdbc.connection.BackendConnection;
import org.apache.shardingsphere.shardingproxy.backend.response.BackendResponse;
import org.apache.shardingsphere.shardingproxy.backend.response.error.ErrorResponse;
import org.apache.shardingsphere.shardingproxy.backend.response.query.QueryData;
import org.apache.shardingsphere.shardingproxy.backend.response.query.QueryHeader;
import org.apache.shardingsphere.shardingproxy.backend.response.query.QueryResponse;
import org.apache.shardingsphere.shardingproxy.backend.response.update.UpdateResponse;
import org.apache.shardingsphere.shardingproxy.backend.text.BackendHandler;
import org.apache.shardingsphere.shardingproxy.context.ShardingProxyContext;
import org.apache.shardingsphere.shardingproxy.frontend.api.QueryCommandExecutor;
import org.apache.shardingsphere.shardingproxy.frontend.postgresql.PostgreSQLErrPacketFactory;
import org.apache.shardingsphere.underlying.common.database.type.DatabaseTypes;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Command bind executor for PostgreSQL.
 */
public final class PostgreSQLComBindExecutor implements QueryCommandExecutor {
    
    private final PostgreSQLComBindPacket packet;
            
    private final BackendHandler binaryProtocolBackendHandler;
    
    private volatile boolean isQuery;
    
    private volatile boolean isErrorResponse;
    
    public PostgreSQLComBindExecutor(final PostgreSQLComBindPacket packet, final BackendConnection backendConnection) {
        this.packet = packet;
        binaryProtocolBackendHandler = null == packet.getSql() ? null
            : BinaryProtocolBackendHandlerFactory.newInstance(DatabaseTypes.getActualDatabaseType("PostgreSQL"), packet.getSql(), packet.getParameters(), backendConnection);
    }
    
    @Override
    public Collection<DatabasePacket> execute() {
        if (ShardingProxyContext.getInstance().isCircuitBreak()) {
            return Collections.singletonList(new PostgreSQLErrorResponsePacket());
        }
        List<DatabasePacket> result = new LinkedList<>();
        result.add(new PostgreSQLBindCompletePacket());
        if (null == binaryProtocolBackendHandler) {
            return result;
        }
        BackendResponse backendResponse = binaryProtocolBackendHandler.execute();
        if (backendResponse instanceof ErrorResponse) {
            isErrorResponse = true;
            result.add(createErrorPacket((ErrorResponse) backendResponse));
        }
        if (backendResponse instanceof UpdateResponse) {
            String generateColumnName = ((UpdateResponse) backendResponse).getGenerateColumnName();
            if (null != generateColumnName) {
                result.add(new PostgreSQLRowDescriptionPacket(1,
                    Collections.singletonList(new PostgreSQLColumnDescription(generateColumnName, 1, Types.BIGINT, 18))));
                result.add(new PostgreSQLDataRowPacket(Collections.singletonList(((UpdateResponse) backendResponse).getLastInsertId())));
            }
            result.add(createUpdatePacket((UpdateResponse) backendResponse));
        }
        if (backendResponse instanceof QueryResponse) {
            createQueryPacket((QueryResponse) backendResponse).ifPresent(result::add);
        }
        return result;
    }
    
    private PostgreSQLErrorResponsePacket createErrorPacket(final ErrorResponse errorResponse) {
        return PostgreSQLErrPacketFactory.newInstance(errorResponse.getCause());
    }
    
    private PostgreSQLCommandCompletePacket createUpdatePacket(final UpdateResponse updateResponse) {
        return new PostgreSQLCommandCompletePacket(updateResponse.getType(), updateResponse.getUpdateCount(), updateResponse.getLastInsertId());
    }
    
    private Optional<PostgreSQLRowDescriptionPacket> createQueryPacket(final QueryResponse queryResponse) {
        List<PostgreSQLColumnDescription> columnDescriptions = getPostgreSQLColumnDescriptions(queryResponse);
        isQuery = !columnDescriptions.isEmpty();
        if (columnDescriptions.isEmpty() || packet.isBinaryRowData()) {
            return Optional.empty();
        }
        return Optional.of(new PostgreSQLRowDescriptionPacket(columnDescriptions.size(), columnDescriptions));
    }
    
    private List<PostgreSQLColumnDescription> getPostgreSQLColumnDescriptions(final QueryResponse queryResponse) {
        List<PostgreSQLColumnDescription> result = new LinkedList<>();
        int columnIndex = 0;
        for (QueryHeader each : queryResponse.getQueryHeaders()) {
            result.add(new PostgreSQLColumnDescription(each.getColumnName(), ++columnIndex, each.getColumnType(), each.getColumnLength()));
        }
        return result;
    }
    
    @Override
    public boolean isQuery() {
        return isQuery;
    }
    
    @Override
    public boolean isSync() {
        return false;
    }
    
    @Override
    public boolean isErrorResponse() {
        return isErrorResponse;
    }
    
    @Override
    public boolean next() throws SQLException {
        return null != binaryProtocolBackendHandler && binaryProtocolBackendHandler.next();
    }
    
    @Override
    public PostgreSQLPacket getQueryData() throws SQLException {
        QueryData queryData = binaryProtocolBackendHandler.getQueryData();
        return packet.isBinaryRowData() ? new PostgreSQLBinaryResultSetRowPacket(queryData.getData(), getPostgreSQLColumnTypes(queryData)) : new PostgreSQLDataRowPacket(queryData.getData());
    }
    
    private List<PostgreSQLColumnType> getPostgreSQLColumnTypes(final QueryData queryData) {
        List<PostgreSQLColumnType> result = new ArrayList<>(queryData.getColumnTypes().size());
        for (int i = 0; i < queryData.getColumnTypes().size(); i++) {
            result.add(PostgreSQLColumnType.valueOfJDBCType(queryData.getColumnTypes().get(i)));
        }
        return result;
    }
}
