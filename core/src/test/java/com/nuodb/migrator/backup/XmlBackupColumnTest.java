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
package com.nuodb.migrator.backup;

import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static com.nuodb.migrator.utils.Equalities.defaultEquality;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;

import org.simpleframework.xml.stream.Format;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.utils.Equality;
import com.nuodb.migrator.utils.xml.XmlPersister;

/**
 * @author Mahesha Godekere
 */
@SuppressWarnings("all")
public class XmlBackupColumnTest {

    private XmlPersister xmlPersister;

    @BeforeMethod
    public void setUp() {
        xmlPersister = new XmlBackupOps() {
            @Override
            protected Format createFormat() {
                return new Format(0);
            }
        }.getXmlPersister();
    }

    @DataProvider(name = "read")
    public Object[][] createReadData() {
        Column column = new Column("column1");
        JdbcType jdbcType =
                new JdbcType(new JdbcTypeDesc(1111, "usertype"), newOptions(0, 0, 0));
        column.setUserDefinedType(true);;
        column.setJdbcType(jdbcType);
        return new Object[][]{
            {
                "<column name=\"column1\" user-type=\"true\">" +
                "<type code=\"1111\" name=\"usertype\" size=\"0\" precision=\"0\" scale=\"0\"/>"+
                "</column>",
                column, defaultEquality()
            }
        };
    }

    @Test(dataProvider = "read")
    public <T> void testRead(String xml, T expected, Equality<T> equality) {
        T actual = xmlPersister.read((Class<T>) expected.getClass(), new StringReader(xml));
        assertTrue(equality.equals(expected, actual),
                format("Actual object does not match expected for xml\n%s", xml));
    }

    @DataProvider(name = "write")
    public Object[][] createWriteData() {
        Column column = new Column("column1");
        JdbcType jdbcType =
                new JdbcType(new JdbcTypeDesc(1111, "usertype"), newOptions(0, 0, 0));
        column.setUserDefinedType(true);
        column.setJdbcType(jdbcType);
        return new Object[][] {
        {
                column,
                "<column name=\"column1\" user-type=\"true\">" +
                "<type code=\"1111\" name=\"usertype\" size=\"0\" precision=\"0\" scale=\"0\"/>"+
                "</column>"
        }};
    }

    @Test(dataProvider = "write")
    public <T> void testWrite(T source, String expected) {
        StringWriter writer = new StringWriter();
        xmlPersister.write(source, writer);
        assertEquals(writer.toString(), expected);
    }
}
