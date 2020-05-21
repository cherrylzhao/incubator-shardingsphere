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

package org.apache.shardingsphere.shardingproxy.backend.binary;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.shardingproxy.backend.binary.admin.BinaryBroadcastBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.binary.query.BinaryQueryBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.communication.jdbc.connection.BackendConnection;
import org.apache.shardingsphere.shardingproxy.backend.text.BackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.text.admin.ShowDatabasesBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.text.admin.UnicastBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.text.admin.UseDatabaseBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.text.sctl.ShardingCTLBackendHandlerFactory;
import org.apache.shardingsphere.shardingproxy.backend.text.sctl.utils.SCTLUtils;
import org.apache.shardingsphere.shardingproxy.backend.text.transaction.SkipBackendHandler;
import org.apache.shardingsphere.shardingproxy.backend.text.transaction.TransactionBackendHandler;
import org.apache.shardingsphere.spi.database.type.DatabaseType;
import org.apache.shardingsphere.sql.parser.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.SetStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.ShowDatabasesStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dal.dialect.mysql.UseStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.BeginTransactionStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.RollbackStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.SetAutoCommitStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.tcl.TCLStatement;
import org.apache.shardingsphere.transaction.core.TransactionOperationType;

import java.util.List;

/**
 * Text protocol backend handler factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BinaryProtocolBackendHandlerFactory {
    
    /**
     * Create new instance of text protocol backend handler.
     *
     * @param databaseType database type
     * @param sql SQL to be executed
     * @param parameters parameters
     * @param backendConnection backend connection
     * @return instance of text protocol backend handler
     */
    public static BackendHandler newInstance(final DatabaseType databaseType, final String sql, final List<Object> parameters, final BackendConnection backendConnection) {
        if (Strings.isNullOrEmpty(sql)) {
            return new SkipBackendHandler();
        }
        final String trimSql = SCTLUtils.trimComment(sql);
        if (trimSql.toUpperCase().startsWith(ShardingCTLBackendHandlerFactory.SCTL)) {
            return ShardingCTLBackendHandlerFactory.newInstance(trimSql, backendConnection);
        }
        SQLStatement sqlStatement = new SQLParserEngine(databaseType.getName()).parse(sql, true);
        if (sqlStatement instanceof TCLStatement) {
            return createTCLBackendHandler(sql, parameters, (TCLStatement) sqlStatement, backendConnection);
        }
        if (sqlStatement instanceof DALStatement) {
            return createDALBackendHandler((DALStatement) sqlStatement, sql, parameters, backendConnection);
        }
        return new BinaryQueryBackendHandler(sql, parameters, backendConnection);
    }
    
    private static BackendHandler createTCLBackendHandler(final String sql, final List<Object> parameters, final TCLStatement tclStatement, final BackendConnection backendConnection) {
        if (tclStatement instanceof BeginTransactionStatement) {
            return new TransactionBackendHandler(TransactionOperationType.BEGIN, backendConnection);
        }
        if (tclStatement instanceof SetAutoCommitStatement) {
            if (((SetAutoCommitStatement) tclStatement).isAutoCommit()) {
                return backendConnection.getStateHandler().isInTransaction() ? new TransactionBackendHandler(TransactionOperationType.COMMIT, backendConnection) : new SkipBackendHandler();
            }
            return new TransactionBackendHandler(TransactionOperationType.BEGIN, backendConnection);
        }
        if (tclStatement instanceof CommitStatement) {
            return new TransactionBackendHandler(TransactionOperationType.COMMIT, backendConnection);
        }
        if (tclStatement instanceof RollbackStatement) {
            return new TransactionBackendHandler(TransactionOperationType.ROLLBACK, backendConnection);
        }
        return new BinaryBroadcastBackendHandler(sql, parameters, backendConnection);
    }
    
    private static BackendHandler createDALBackendHandler(final DALStatement dalStatement, final String sql, final List<Object> parameters, final BackendConnection backendConnection) {
        if (dalStatement instanceof UseStatement) {
            return new UseDatabaseBackendHandler((UseStatement) dalStatement, backendConnection);
        }
        if (dalStatement instanceof ShowDatabasesStatement) {
            return new ShowDatabasesBackendHandler(backendConnection);
        }
        // FIXME: There are three SetStatement class.
        if (dalStatement instanceof SetStatement) {
            return new BinaryBroadcastBackendHandler(sql, parameters, backendConnection);
        }
        return new UnicastBackendHandler(sql, backendConnection);
    }
}
