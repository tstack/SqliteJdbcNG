/*
 * Copyright (c) 2013, Timothy Stack
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY Tim Stack AND CONTRIBUTORS ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sqlitejdbcng;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Savepoint;

public class SqliteSavepoint implements Savepoint {
    private final int id;
    private final String name;
    private final String sqliteName;

    public SqliteSavepoint(int id) {
        this.id = id;
        this.name = null;
        this.sqliteName = "_sp_" + id;
    }

    public SqliteSavepoint(String name) {
        this.id = 0;
        this.name = name;
        this.sqliteName = name;
    }

    String getSqliteName() {
        return this.sqliteName;
    }

    @Override
    public int getSavepointId() throws SQLException {
        if (this.name != null)
            throw new SQLNonTransientException("Named savepoints do not have an ID");

        return this.id;
    }

    @Override
    public String getSavepointName() throws SQLException {
        if (this.name == null)
            throw new SQLNonTransientException("Anonymous savepoints do not have a name");

        return this.name;
    }
}
