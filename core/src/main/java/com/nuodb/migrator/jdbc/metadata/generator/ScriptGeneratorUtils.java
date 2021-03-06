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

import com.google.common.base.Function;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Schema;

import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singleton;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class ScriptGeneratorUtils {

    private static final String COMMA = ", ";

    public static String getUseSchema(Schema schema, ScriptGeneratorManager scriptGeneratorManager) {
        String useSchema = null;
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        if (scriptGeneratorManager.getTargetSchema() != null) {
            useSchema = dialect.getUseSchema(scriptGeneratorManager.getTargetSchema(), true);
        } else if (scriptGeneratorManager.getTargetCatalog() != null) {
            useSchema = dialect.getUseSchema(scriptGeneratorManager.getTargetCatalog(), true);
        }
        if (useSchema == null) {
            useSchema = schema.getIdentifier() != null ?
                    dialect.getUseSchema(scriptGeneratorManager.getName(schema)) :
                    dialect.getUseSchema(scriptGeneratorManager.getName(schema.getCatalog()));
        }
        return useSchema;
    }

    public static String getDropSchema(Schema schema, ScriptGeneratorManager scriptGeneratorManager) {
        String dropSchema = null;
        Dialect dialect = scriptGeneratorManager.getTargetDialect();
        if (scriptGeneratorManager.getTargetSchema() != null) {
            dropSchema = dialect.getDropSchema(scriptGeneratorManager.getTargetSchema(), true);
        } else if (scriptGeneratorManager.getTargetCatalog() != null) {
            dropSchema = dialect.getDropSchema(scriptGeneratorManager.getTargetCatalog(), true);
        }
        if (dropSchema == null) {
            dropSchema = schema.getIdentifier() != null ?
                    dialect.getDropSchema(scriptGeneratorManager.getName(schema)) :
                    dialect.getDropSchema(scriptGeneratorManager.getName(schema.getCatalog()));
        }
        return dropSchema;
    }

    public static Collection<String> getCreateMultipleIndexes(Collection<Index> indexes,
                                                              final ScriptGeneratorManager scriptGeneratorManager) {
        return singleton(join(transform(indexes, new Function<Index, String>() {
            @Override
            public String apply(Index index) {
                return get(scriptGeneratorManager.getCreateScripts(index), 0);
            }
        }), COMMA));
    }
}
