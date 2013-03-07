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
package com.nuodb.migrator.jdbc.metadata.generator;

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.CREATE;
import static com.nuodb.migrator.jdbc.metadata.generator.ScriptType.DROP;
import static java.util.Collections.emptySet;

/**
 * @author Sergey Bushik
 */
public abstract class ScriptGeneratorBase<T extends MetaData> extends MetaDataHandlerBase implements ScriptGenerator<T> {

    protected ScriptGeneratorBase(Class<T> objectClass) {
        super(objectClass);
    }

    @Override
    public Collection<String> getScripts(T object, ScriptGeneratorContext scriptGeneratorContext) {
        Collection<String> scripts;
        if (isGenerateScript(scriptGeneratorContext, DROP) && isGenerateScript(scriptGeneratorContext, CREATE)) {
            scripts = getDropCreateScripts(object, scriptGeneratorContext);
        } else if (isGenerateScript(scriptGeneratorContext, DROP)) {
            scripts = getDropScripts(object, scriptGeneratorContext);
        } else if (isGenerateScript(scriptGeneratorContext, CREATE)) {
            scripts = getCreateScripts(object, scriptGeneratorContext);
        } else {
            scripts = emptySet();
        }
        return scripts;
    }

    protected abstract Collection<String> getDropScripts(T object, ScriptGeneratorContext scriptGeneratorContext);

    protected abstract Collection<String> getCreateScripts(T object, ScriptGeneratorContext scriptGeneratorContext);

    protected Collection<String> getDropCreateScripts(T object, ScriptGeneratorContext scriptGeneratorContext) {
        Collection<String> scripts = newArrayList();
        scripts.addAll(getDropScripts(object, scriptGeneratorContext));
        scripts.addAll(getCreateScripts(object, scriptGeneratorContext));
        return scripts;
    }

    protected boolean isGenerateScript(ScriptGeneratorContext scriptGeneratorContext, ScriptType scriptType) {
        Collection<ScriptType> scriptTypes = scriptGeneratorContext.getScriptTypes();
        return scriptTypes != null && scriptTypes.contains(scriptType);
    }
}