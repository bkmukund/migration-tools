/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.query.SelectQuery;
import com.nuodb.migrator.jdbc.query.StatementAction;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

/**
 * @author Mukund 
 */
public class MSSQLServerDatabaseInspector extends SimpleDatabaseInspector {

    /** 
     * This method is overridden to build a query to fetch Database collation information from MSSQLSERVER 
     * sys.databases table 
     */
    @Override
    protected String getCollation(final InspectionContext inspectionContext, Database database) throws SQLException {
        String collation = null;
        StatementTemplate template = new StatementTemplate(inspectionContext.getConnection());
        collation = template.executeStatement(
                new StatementFactory<PreparedStatement>() {
                    @Override
                    public PreparedStatement createStatement(Connection connection) throws SQLException {
                        SelectQuery collationQuery = new SelectQuery();
                        collationQuery.column("collation_name");
                        collationQuery.from("sys.databases");
                        collationQuery.where("name= ?");
                        return connection.prepareStatement(collationQuery.toString());
                    }
                }, new StatementAction<PreparedStatement, String>() {
                    @Override
                    public String executeStatement(PreparedStatement statement) throws SQLException {
                        statement.setString(1, inspectionContext.getConnection().getCatalog());
                        ResultSet collations = statement.executeQuery();
                        String collation = null;
                        if (collations.next()) {
                            collation = collations.getString(1);
                        }
                        return collation;
                    }
                }
            );
        return collation;
    }
}
