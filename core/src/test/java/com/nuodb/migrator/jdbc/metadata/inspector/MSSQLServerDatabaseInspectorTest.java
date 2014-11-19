package com.nuodb.migrator.jdbc.metadata.inspector;
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

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.jdbc.metadata.Database;

/**
 * @author Mukund
 */

public class MSSQLServerDatabaseInspectorTest extends InspectorTestBase {

    public MSSQLServerDatabaseInspectorTest() {
        super(MSSQLServerDatabaseInspector.class);
    }

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
    }

    @DataProvider(name = "getDatabaseData")
    public Object[][] createGetCollationData() throws Exception{
        Database database = new Database();
        database.setName("test");
        database.setEncoding("latin1");
        
        Database database1 = new Database();
        database.setName("test1");
        database.setEncoding("SQL_latin1");
        return new Object[][] {
                { database } ,
                { database1 }
        };
    }

    @Test(dataProvider = "getDatabaseData")
    public void testDatabaseCollation(Database db) throws Exception {
        configureDatabaseCollationResultSet(db);
        configureDatabaseInfo(db);
        InspectionResults inspectionResults = getInspectionManager().inspect(getConnection(), DATABASE);
        Collection<Database> dbs = inspectionResults.getObjects(DATABASE);
        
        assertNotNull(dbs);
        assertEquals(dbs.size(), 1);
        assertEquals(get(dbs, 0).getEncoding(), db.getEncoding());
}

    private ResultSet configureDatabaseCollationResultSet(Database db) throws Exception{
        PreparedStatement query = mock(PreparedStatement.class);
        given(getConnection().prepareStatement(anyString(), anyInt(), anyInt())).willReturn(query);
        given(getConnection().prepareStatement(anyString())).willReturn(query);

        ResultSet dbResultSet = mock(ResultSet.class);
        given(query.executeQuery()).willReturn(dbResultSet);
        given(dbResultSet.next()).willReturn(true, false);
        given(dbResultSet.getString(1)).willReturn(db.getEncoding());
        return dbResultSet;
    }

    private void configureDatabaseInfo(Database database) throws Exception{
        InspectionContext context = mock(InspectionContext.class);
        InspectionResults inspectionResults = mock(InspectionResults.class);
        given(context.getInspectionResults()).willReturn(inspectionResults);
        DatabaseMetaData dbmd = mock(DatabaseMetaData.class);
        given(getConnection().getMetaData()).willReturn(dbmd);
        given(inspectionResults.getObject(DATABASE)).willReturn(database);
    }
}
