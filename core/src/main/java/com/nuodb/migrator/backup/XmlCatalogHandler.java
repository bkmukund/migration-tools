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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.jdbc.metadata.*;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadTargetAwareContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlCatalogHandler extends XmlIdentifiableHandlerBase<Catalog> implements XmlConstants {

    private static final String SCHEMA_ELEMENT = "schema";

    public XmlCatalogHandler() {
        super(Catalog.class);
    }

    @Override
    protected Catalog createTarget(InputNode input, Class<? extends Catalog> type) {
        return null;
    }

    @Override
    protected void readAttributes(InputNode input, XmlReadTargetAwareContext<Catalog> context) throws Exception {
        String name = context.readAttribute(input, NAME_ATTRIBUTE, String.class);
        Database database = getParentTarget(context);
        context.setTarget(database.hasCatalog(name) ? database.getCatalog(name) : database.addCatalog(name));
    }

    @Override
    protected void readElement(InputNode input, Catalog catalog, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (SCHEMA_ELEMENT.equals(element)) {
            catalog.addSchema(context.read(input, Schema.class));
        }
    }

    @Override
    protected void writeElements(OutputNode output, Catalog catalog, XmlWriteContext context) throws Exception {
        for (Schema schema : catalog.getSchemas()) {
            context.writeElement(output, SCHEMA_ELEMENT, schema);
        }
    }
}
