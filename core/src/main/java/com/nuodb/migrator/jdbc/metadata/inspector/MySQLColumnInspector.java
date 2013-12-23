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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.query.StatementCallback;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.inspector.InspectionResultsUtils.addTable;
import static com.nuodb.migrator.jdbc.query.QueryUtils.where;
import static org.apache.commons.lang3.StringUtils.containsAny;

/**
 * @author Sergey Bushik
 */
public class MySQLColumnInspector extends SimpleColumnInspector {

    @Override
    protected void inspect(final InspectionContext inspectionContext, TableInspectionScope inspectionScope)
            throws SQLException {
        super.inspect(inspectionContext, inspectionScope);

        final InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        final StringBuilder query = new StringBuilder(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, " +
                        "COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS");
        final Collection<String> filters = newArrayList();
        final Collection<String> parameters = newArrayList();
        String catalogName = inspectionScope.getCatalog();
        if (catalogName != null) {
            filters.add(containsAny(inspectionScope.getCatalog(), "%") ? "TABLE_SCHEMA LIKE ?" :
                    "TABLE_SCHEMA=?");
            parameters.add(catalogName);
        } else {
            filters.add("TABLE_SCHEMA=DATABASE()");
        }
        String tableName = inspectionScope.getTable();
        if (tableName != null) {
            filters.add(containsAny(inspectionScope.getCatalog(), "%") ? "TABLE_NAME LIKE ?" : "TABLE_NAME=?");
            parameters.add(tableName);
        }
        // finally append filters to the query and join then with "AND"
        where(query, filters, "AND");
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        template.execute(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement create(Connection connection) throws SQLException {
                        // let's prepare statement from the query
                        return connection.prepareStatement(query.toString());
                    }
                },
                new StatementCallback<PreparedStatement>() {
                    @Override
                    public void process(PreparedStatement statement) throws SQLException {
                        int index = 1;
                        for (String parameter : parameters) {
                            statement.setString(index++, parameter);
                        }
                        ResultSet columns = statement.executeQuery();

                        while (columns.next()) {
                            Table table = addTable(inspectionResults, columns.getString("TABLE_SCHEMA"), null,
                                    columns.getString("TABLE_NAME"));
                            Column column = table.addColumn(columns.getString("COLUMN_NAME"));
                            column.setColumnType(columns.getString("COLUMN_TYPE"));
                        }
                    }
                }
        );
    }
}