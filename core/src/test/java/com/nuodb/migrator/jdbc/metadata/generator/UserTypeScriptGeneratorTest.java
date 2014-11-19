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
package com.nuodb.migrator.jdbc.metadata.generator;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.ORACLE;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptGeneratorManager.SCRIPTS_IN_CREATE_TABLE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.session.SessionUtils.createSession;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.Collection;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.NuoDBDialect;
import com.nuodb.migrator.jdbc.dialect.OracleDialect;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.Table;

/**
 * @author Mahesha Godekere
 */
public class UserTypeScriptGeneratorTest {

    private ScriptGeneratorManager scriptGeneratorManager;

    @BeforeMethod
    public void setUp() throws SQLException {
        scriptGeneratorManager = new ScriptGeneratorManager();
        scriptGeneratorManager.addAttribute(SCRIPTS_IN_CREATE_TABLE, true);
        scriptGeneratorManager.setSourceSession(createSession(new OracleDialect(ORACLE)));
    }

    @DataProvider(name = "getScripts")
    public Object[][] createGetScriptsData() {
        // create & drop scripts from a source mysql table
        Database database1 = new Database();
        database1.setDialect(new OracleDialect(new DatabaseInfo("Oracle")));
        Catalog catalog = database1.addCatalog(valueOf("catalog"));

        Table table = new Table("users");
        table.setDatabase(database1);
        
        Column col = table.addColumn("usertype");
        col.setTypeCode(1111);
        col.setTypeName("usertype");
        col.setNullable(true);
        col.setPosition(1);
        col.setUserDefinedType(true);
        
        catalog.addSchema((String) null).addTable(table);
        
        Collection<Object[]> data = newArrayList();
        data.add(new Object[]{table, newArrayList(CREATE), null,
                newArrayList(
                        "CREATE TABLE \"users\" (\"usertype\" BLOB)")});
        return data.toArray(new Object[][]{});
    }

    @Test(dataProvider = "getScripts")
    public void testGetScripts(MetaData object, Collection<ScriptType> scriptTypes, Dialect targetDialect,
                               Collection<String> expected) throws Exception {
        scriptGeneratorManager.setTargetDialect(targetDialect == null ? createTargetDialect() : targetDialect);
        scriptGeneratorManager.setScriptTypes(scriptTypes);

        Collection<String> scripts = scriptGeneratorManager.getScripts(object);
        assertNotNull(scripts);
        assertEquals(newArrayList(expected), newArrayList(scripts));
    }

    private Dialect createTargetDialect() {
        NuoDBDialect dialect = new NuoDBDialect();
        Database database = new Database();
        database.setDialect(dialect);
        return dialect;
    }
}