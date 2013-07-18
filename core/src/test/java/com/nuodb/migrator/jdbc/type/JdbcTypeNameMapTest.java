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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.jdbc.type.jdbc2.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Types;

import static com.nuodb.migrator.jdbc.type.JdbcTypeSpecifiers.*;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeNameMapTest {

    private JdbcTypeNameMap jdbcTypeNameMap = new JdbcTypeNameMap();

    @BeforeMethod
    public void setUp() {
        jdbcTypeNameMap.addJdbcTypeName(Types.DOUBLE, "DOUBLE");
        jdbcTypeNameMap.addJdbcTypeName(Types.TIME, newScale(0), "TIME");
        jdbcTypeNameMap.addJdbcTypeName(Types.TIME, "TIME({S})");
        jdbcTypeNameMap.addJdbcTypeName(Types.DECIMAL, "DECIMAL({P},{S})");
        jdbcTypeNameMap.addJdbcTypeName(Types.CHAR, "CHAR({N})");
        jdbcTypeNameMap.addJdbcTypeName(Types.VARCHAR, "VARCHAR({N})");
        jdbcTypeNameMap.addJdbcTypeName(Types.BIGINT, "BIGINT");
        jdbcTypeNameMap.addJdbcTypeName(Types.BIGINT, newPrecision(20), "NUMBER({P})");
        jdbcTypeNameMap.addJdbcTypeName(Types.BIT, "BIT({N})");
        jdbcTypeNameMap.addJdbcTypeName(Types.BIT, newSize(1), "BOOLEAN");
    }

    @DataProvider(name = "getTypeName")
    public Object[][] createGetTypeNameData() {
        return new Object[][]{
                {JdbcDoubleType.INSTANCE, newSpecifiers(8, 8, 0), "DOUBLE"},
                {JdbcDoubleType.INSTANCE, newSpecifiers(8, 6, 2), "DOUBLE"},
                {JdbcTimeType.INSTANCE, newSpecifiers(19, 19, 0), "TIME"},
                {JdbcTimeType.INSTANCE, newSpecifiers(19, 15, 4), "TIME(4)"},
                {JdbcDecimalType.INSTANCE, newSpecifiers(8, 8, 0), "DECIMAL(8,0)"},
                {JdbcDecimalType.INSTANCE, newSpecifiers(8, 6, 2), "DECIMAL(6,2)"},
                {JdbcCharType.INSTANCE, newSize(1), "CHAR(1)"},
                {JdbcVarCharType.INSTANCE, newSize(128), "VARCHAR(128)"},
                {JdbcLongVarCharType.INSTANCE, newSize(128), null},
                {JdbcBigIntType.INSTANCE, newPrecision(10), "BIGINT"},
                {JdbcBigIntType.INSTANCE, newPrecision(15), "BIGINT"},
                {JdbcBigIntType.INSTANCE, newPrecision(19), "BIGINT"},
                {JdbcBigIntType.INSTANCE, newPrecision(20), "NUMBER(20)"},
                {JdbcBitType.INSTANCE, newSize(1), "BOOLEAN"},
                {JdbcBitType.INSTANCE, newSize(10), "BIT(10)"},
        };
    }

    @Test(dataProvider = "getTypeName")
    public void testGetTypeName(JdbcType type, JdbcTypeSpecifiers specifiers, String typeName) {
        assertEquals(jdbcTypeNameMap.getJdbcTypeName(type.getJdbcTypeDesc(), specifiers), typeName);
    }
}
