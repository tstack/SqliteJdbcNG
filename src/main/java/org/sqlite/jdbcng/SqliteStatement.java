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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Level.WARNING;

public class SqliteStatement extends SqliteCommon implements Statement {
    private static final Logger LOGGER = Logger.getLogger(SqliteConnection.class.getName());

    protected final SqliteConnection conn;
    protected final List<String> batchList = new ArrayList<>();
    protected boolean closeOnCompletion;
    protected String lastQuery;
    protected SqliteResultSet lastResult;
    protected int lastUpdateCount;
    protected boolean closed;

    public SqliteStatement(SqliteConnection conn) {
        this.conn = conn;
    }

    void requireOpened() throws SQLException {
        if (this.closed) {
            throw new SQLNonTransientException("Statement is closed for business");
        }
    }

    Pointer<Sqlite3.Statement> requireAccess(Pointer<Sqlite3.Statement> stmt) throws SQLException {
        stmt = Sqlite3.withReleaser(stmt);

        if (this.conn.isReadOnly() && Sqlite3.sqlite3_stmt_readonly(stmt) == 0) {
            stmt.release();
            throw new SQLNonTransientException(
                    "Connection is in read-only mode, but statement is not read-only");
        }

        return stmt;
    }

    String getLastQuery() {
        return this.lastQuery;
    }

    void resultSetClosed() throws SQLException {
        if (this.closeOnCompletion) {
            this.close();
        }
    }

    void replaceResultSet(SqliteResultSet rs) throws SQLException {
        if (this.lastResult != null) {
            this.lastResult.close();
        }
        this.lastResult = rs;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        requireOpened();

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        this.clearWarnings();

        this.lastQuery = s;

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL),
                this.conn.getHandle());

        Pointer<Sqlite3.Statement> stmt = Sqlite3.withReleaser(stmt_out.get());

        if (Sqlite3.sqlite3_stmt_readonly(stmt) == 0)
            throw new SQLNonTransientException("SQL statement is not a query");

        this.replaceResultSet(new SqliteResultSet(this, stmt));

        return this.lastResult;
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        this.execute(s);

        return this.lastUpdateCount;
    }

    @Override
    public void close() throws SQLException {
        if (this.lastResult != null) {
            Pointer<Sqlite3.Statement> stmt = this.lastResult.getHandle();

            this.lastResult.close();
            this.lastResult = null;
            stmt.release();
            this.conn.statementClosed(this);
        }

        this.closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
    }

    @Override
    public void cancel() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCursorName(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean execute(String s) throws SQLException {
        requireOpened();

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        this.clearWarnings();

        this.lastQuery = s;
        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL),
                this.conn.getHandle());

        Pointer<Sqlite3.Statement> stmt = Sqlite3.withReleaser(stmt_out.get());

        try {
            int rc = Sqlite3.sqlite3_step(stmt);

            switch (Sqlite3.ReturnCodes.valueOf(rc)) {
                case SQLITE_OK:
                case SQLITE_DONE:
                    break;
                default:
                    Sqlite3.checkOk(rc, this.conn.getHandle());
                    break;
            }

            if (Sqlite3.sqlite3_stmt_readonly(stmt) != 0) {
                this.replaceResultSet(new SqliteResultSet(this, stmt));
                stmt = null;
            }
            else {
                this.replaceResultSet(null);
            }
        }
        finally {
            Pointer.release(stmt);
        }

        this.lastUpdateCount = Sqlite3.sqlite3_changes(this.conn.getHandle());

        return this.lastResult != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        requireOpened();

        return this.lastResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        requireOpened();

        return this.lastUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        requireOpened();

        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        requireOpened();

        if (direction != ResultSet.FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("SQLite result sets only support forward fetching");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        requireOpened();

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        requireOpened();
    }

    @Override
    public int getFetchSize() throws SQLException {
        requireOpened();

        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        requireOpened();

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        requireOpened();

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String s) throws SQLException {
        requireOpened();

        this.batchList.add(s);
    }

    @Override
    public void clearBatch() throws SQLException {
        requireOpened();

        this.batchList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Connection getConnection() throws SQLException {
        requireOpened();

        return this.conn;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        requireOpened();

        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support generated keys");
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        requireOpened();

        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        requireOpened();

        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        requireOpened();

        return this.closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void finalize() throws Throwable {
        if (!this.closed) {
            LOGGER.log(WARNING,
                    "SQLite database statement was not explicitly closed -- {0}",
                    this.conn.getURL());

            this.close();
        }
    }

    @Override
    public String toString() {
        return this.lastQuery;
    }
}
