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
package com.nuodb.migrator.jdbc.type;

import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createColumn;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.ResultSet;

import org.mockito.Mock;
import org.testng.annotations.Test;

import com.nuodb.migrator.backup.format.value.JdbcValueFormat;
import com.nuodb.migrator.backup.format.value.ValueUtils;
import com.nuodb.migrator.jdbc.metadata.Column;

/**
 * @author Mahesha Godekere
 */

public class JdbcUserTypeValueAccessTest {

    private JdbcTypeRegistry jdbcTypeRegistry = new SimpleJdbcTypeRegistry();
    @Mock
    private Connection connection;
    @Mock
    private ResultSet resultSet;

    @Test
    public void testGetColumnValueGetter() throws Exception{
        Column column = createColumn("catalog", "schema", "table", "column");
        column.setTypeCode(1111);
        column.setTypeName("usertype");
        column.setUserDefinedType(true);

        SimpleJdbcValueAccess<Object> access = new SimpleJdbcValueAccess<Object>(
                (JdbcValueGetter<Object>) getJdbcValueGetter(1111, "usertype"),
                connection, resultSet, 1, column);

        assertNotNull(access);

        Object actual = new JdbcValueFormat().getValue(access, null);
        Object expected = ValueUtils.string(null);
        
        assertEquals(expected,actual);
    }

    @SuppressWarnings("unchecked")
    private <T> JdbcValueGetter<T> getJdbcValueGetter(int typeCode, String typeName) {
        return new SimpleJdbcValueGetter<T>(jdbcTypeRegistry, 
                jdbcTypeRegistry.getJdbcType(new JdbcTypeDesc(typeCode,typeName), false));

    }
}
