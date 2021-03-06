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
package com.nuodb.migrator.jdbc.session;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Sergey Bushik
 */
public abstract class SessionFactoryBase implements SessionFactory {

    private Collection<SessionObserver> sessionObservers = newArrayList();

    @Override
    public Session openSession() throws SQLException {
        return openSession(newHashMap());
    }

    @Override
    public Session openSession(Map<Object, Object> context) throws SQLException {
        Session session = open(context);
        afterOpen(session);
        return session;
    }

    protected abstract Session open(Map<Object, Object> context) throws SQLException;

    @Override
    public void addSessionObserver(SessionObserver sessionObserver) {
        sessionObservers.add(sessionObserver);
    }

    @Override
    public void removeSessionObserver(SessionObserver sessionObserver) {
        sessionObservers.remove(sessionObserver);
    }

    protected void afterOpen(Session session) throws SQLException {
        for (SessionObserver sessionObserver : sessionObservers) {
            sessionObserver.afterOpen(session);
        }
    }

    protected void beforeClose(Session session) throws SQLException {
        for (SessionObserver sessionObserver : sessionObservers) {
            sessionObserver.beforeClose(session);
        }
    }

    public void closeSession(Session session) throws SQLException {
        beforeClose(session);
        close(session);
    }

    protected abstract void close(Session session) throws SQLException;
}
