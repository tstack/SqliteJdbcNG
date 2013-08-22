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
 *  Neither the name of Timothy Stack nor the names of its contributors
 * may be used to endorse or promote products derived from this software
 * without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ''AS IS'' AND ANY
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

package org.sqlite.jdbcng;

import org.bridj.Pointer;
import org.sqlite.jdbcng.bridj.Sqlite3;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;

public class SqliteParameterMetadata implements ParameterMetaData {
    private final SqlitePreparedStatement parent;
    private final Pointer<Sqlite3.Statement> stmt;
    private final int parameterCount;
    private final int precision;

    public SqliteParameterMetadata(SqlitePreparedStatement parent, Pointer<Sqlite3.Statement> stmt) {
        this.parent = parent;
        this.stmt = stmt;
        this.parameterCount = Sqlite3.sqlite3_bind_parameter_count(this.stmt);
        this.precision = Sqlite3.sqlite3_limit(this.parent.conn.getHandle(), Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return this.parameterCount;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return true;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return this.precision;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return Types.VARCHAR;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return "java.lang.String";
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLNonTransientException("No object implements the given class");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
