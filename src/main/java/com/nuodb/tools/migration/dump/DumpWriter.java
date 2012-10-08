/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.tools.migration.dump;

import com.nuodb.tools.migration.dump.output.CsvOutputFormat;
import com.nuodb.tools.migration.dump.output.OutputFormat;
import com.nuodb.tools.migration.dump.query.Query;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.dump.query.StatementQuery;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.connection.DriverManagerConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.*;
import com.nuodb.tools.migration.spec.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.nuodb.tools.migration.jdbc.metamodel.ObjectType.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpWriter {

    private final Log log = LogFactory.getLog(getClass());

    public void write(DumpSpec dump) throws SQLException {
        // TODO: 1) transaction context should be created / obtained ?
        // TODO: 2) statistics event listener reference to publish events to
        ConnectionSpec spec = dump.getConnectionSpec();
        ConnectionProvider provider = getConnectionProvider(spec);
        Connection connection = provider.getConnection();
        try {
            write(dump, connection, spec.getCatalog(), spec.getSchema());
        } finally {
            provider.closeConnection(connection);
        }
    }

    public void write(DumpSpec dump, Connection connection, String catalog, String schema) throws SQLException {
        DatabaseIntrospector introspector = new DatabaseIntrospector();
        introspector.withCatalog(catalog);
        introspector.withSchema(schema);
        introspector.withConnection(connection);
        introspector.withObjectTypes(CATALOG, SCHEMA, TABLE, COLUMN);
        Database database = introspector.introspect();
        List<Query> queries = createQueries(database, dump);
        for (Query query : queries) {
            PreparedStatement statement = connection.prepareStatement(query.toQueryString());
            if (log.isDebugEnabled()) {
                log.debug(String.format("Executing statement %1$s", query.toQueryString()));
            }
            ResultSet resultSet = statement.executeQuery();
            // TODO: create output based on output spec
            OutputFormat output = new CsvOutputFormat();
            output.open(System.out);
            output.init(resultSet);
            while (resultSet.next()) {
                output.row(resultSet);
            }
            output.end();
        }
    }

    protected ConnectionProvider getConnectionProvider(ConnectionSpec spec) {
        DriverManagerConnectionSpec driverManagerConnectionSpec = (DriverManagerConnectionSpec) spec;
        return new DriverManagerConnectionProvider(driverManagerConnectionSpec, false, Connection.TRANSACTION_READ_COMMITTED);
    }

    protected List<Query> createQueries(Database database, DumpSpec dump) {
        Collection<Table> tables = database.listTables();
        List<Query> queries = new ArrayList<Query>();
        Collection<TableSpec> tableSpecs = dump.getTableSpecs();
        if (tableSpecs != null) {
            if (tableSpecs.isEmpty()) {
                queries.addAll(createSelectQueries(tables, null));
            } else {
                for (TableSpec tableSpec : tableSpecs) {
                    queries.addAll(createSelectQueries(tables, tableSpec));
                }
            }
        }
        Collection<QuerySpec> querySpecs = dump.getQuerySpecs();
        if (querySpecs != null) {
            for (QuerySpec querySpec : querySpecs) {
                queries.add(createStatementQuery(querySpec));
            }
        }
        return queries;
    }

    protected List<Query> createSelectQueries(Collection<Table> tables, TableSpec tableSpec) {
        List<Query> selectQueries = new ArrayList<Query>();
        String tableName = tableSpec != null ? tableSpec.getName() : null;
        for (Table table : tables) {
            if (tableName == null || tableName.equals(table.getName().value())) {
                selectQueries.add(createSelectQuery(table, tableSpec));
            }
        }
        return selectQueries;
    }

    protected Query createSelectQuery(Table table, TableSpec tableSpec) {
        SelectQuery selectQuery = new SelectQuery();
        List<ColumnSpec> columnSpecs = tableSpec != null ? tableSpec.getColumnSpecs() : null;
        Collection<Column> columns;
        if (columnSpecs == null || columnSpecs.isEmpty()) {
            columns = table.listColumns();
        } else {
            columns = new ArrayList<Column>();
            for (ColumnSpec columnSpec : columnSpecs) {
                for (Column column : table.listColumns()) {
                    if (columnSpec.getName().equals(column.getName().value())) {
                        columns.add(column);
                    }
                }
            }
        }
        for (Column column : columns) {
            selectQuery.addColumn(column);
        }
        selectQuery.addTable(table);
        if (tableSpec != null && tableSpec.getCondition() != null) {
            selectQuery.addCondition(tableSpec.getCondition());
        }
        return selectQuery;
    }

    protected Query createStatementQuery(QuerySpec querySpec) {
        return new StatementQuery(querySpec.getStatement());
    }
}