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

package org.sqlitejdbcng;

import org.bridj.Pointer;
import org.sqlitejdbcng.bridj.Sqlite3;
import org.sqlitejdbcng.internal.TimeoutProgressCallback;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqliteStatement extends SqliteCommon implements Statement {
    private static final Logger LOGGER = Logger.getLogger(SqliteConnection.class.getName());

    protected final SqliteConnection conn;
    protected final List<String> batchList = new ArrayList<String>();
    protected int queryTimeoutSeconds;
    protected boolean closeOnCompletion;
    protected boolean escapeStatements = true;
    protected String lastQuery;
    protected int maxRows;
    protected SqliteResultSet lastResult;
    protected int lastUpdateCount;
    protected boolean closed;
    protected final TimeoutProgressCallback timeoutCallback;

    public SqliteStatement(SqliteConnection conn) {
        this.conn = conn;
        this.timeoutCallback = new TimeoutProgressCallback(conn);
    }

    Pointer<Sqlite3.Sqlite3Db> getDbHandle() {
        return this.conn.getHandle();
    }

    void requireOpened() throws SQLException {
        if (this.isClosed()) {
            throw new SQLNonTransientException("Statement is closed for business");
        }
    }

    void resultSetClosed() throws SQLException {
        if (this.closeOnCompletion) {
            this.close();
        }
    }

    void replaceResultSet(SqliteResultSet rs) throws SQLException {
        if (this.lastResult != null) {
            this.lastResult.close();
            this.lastResult = null;
        }
        if (rs != null) {
            this.lastUpdateCount = -1;

            // Perform an initial sqlite3_step() here so that the statement is
            // actually executed.  This behavior is expected when using sqlite
            // extensions that provide functions with side-effects that don't
            // return anything in a result set.
            try {
                rs.step();
            }
            catch (SQLException e) {
                rs.close();
                throw e;
            }
        }
        this.lastResult = rs;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        requireOpened();
        this.clearWarnings();

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);
        String escapedString = this.escapeStatements ? this.conn.nativeSQL(s) : s;

        this.lastQuery = s;

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(escapedString), -1, stmt_out, Pointer.NULL),
                this.conn.getHandle());

        Pointer<Sqlite3.Statement> stmt = stmt_out.get();

        if (Sqlite3.sqlite3_column_count(stmt) == 0) {
            Sqlite3.sqlite3_finalize(stmt);
            throw new SQLNonTransientException("SQL statement is not a query");
        }

        SqliteResultSetMetadata metadata = new SqliteResultSetMetadata(stmt);

        this.replaceResultSet(new SqliteResultSet(this, metadata, stmt, this.maxRows));

        return this.lastResult;
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        if (this.execute(s)) {
            ResultSet rs = null;

            try {
                rs = this.getResultSet();
                /*
                 * Certain statements are read-only, but are not SELECT
                 * queries.  For example, adding another database to a
                 * connection with "ATTACH".  This means we need to get
                 * the result set and execute it at least once.
                 */
                if (rs.next()) {
                    LOGGER.log(Level.WARNING,
                            "executeUpdate used with a statement that is returning results -- {0}",
                            new Object[] { s });
                }
            } finally {
                closeQuietly(rs);
            }
        }

        return this.lastUpdateCount;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!this.closed) {
            this.closed = true;

            if (this.lastResult != null) {
                this.lastResult.close();
                this.lastResult = null;
            }

            this.conn.statementClosed(this);
        }
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
        requireOpened();

        return this.maxRows;
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        requireOpened();

        if (i < 0)
            throw new SQLNonTransientException("maxRows must be greater than or equal to zero");

        this.maxRows = i;
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        this.escapeStatements = b;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        requireOpened();

        return this.queryTimeoutSeconds;
    }

    /**
     * Sets a timeout for any executed statements.
     *
     * Implementation details:
     * - The timeout is implemented through the progress callback
     *   mechanism in SQLite, so it applies to all statements concurrently
     *   running on a connection.  However, connections should not be shared
     *   across threads, so this shouldn't be a problem.
     * - For queries executed through executeQuery(), the statement is not
     *   executed until ResultSet.next() is run and the timeout is applied
     *   at that time.  The next() method will also be the one throwing the
     *   exception.
     * - If a timeout is applied to a statement and cancel() is called, the
     *   driver will throw a SQLTimeoutException since the SQLite library
     *   does not distinguish between sqlite3_interrupt() being called and
     *   a progress handler returning a non-zero return code.
     *
     * {@inheritDoc}
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        requireOpened();

        if (seconds < 0)
            throw new SQLNonTransientException("Timeout must be greater than or equal to zero");

        this.queryTimeoutSeconds = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        requireOpened();

        Sqlite3.sqlite3_interrupt(this.conn.getHandle());
    }

    @Override
    public void setCursorName(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support named cursors");
    }

    @Override
    public boolean execute(String s) throws SQLException {
        int changeDiff = 0;

        requireOpened();
        this.clearWarnings();

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);
        String escapedString = this.escapeStatements ? this.conn.nativeSQL(s) : s;

        this.lastQuery = s;
        this.lastUpdateCount = -1;
        this.replaceResultSet(null);
        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(escapedString), -1, stmt_out, Pointer.NULL),
                this.conn.getHandle());

        Pointer<Sqlite3.Statement> stmt = stmt_out.get();

        try {
            if (Sqlite3.sqlite3_column_count(stmt) != 0) {
                try {
                    SqliteResultSetMetadata metadata = new SqliteResultSetMetadata(stmt);

                    this.replaceResultSet(new SqliteResultSet(this, metadata, stmt, this.maxRows));
                }
                finally {
                    stmt = null;
                }
            }
            else {
                TimeoutProgressCallback cb = null;
                int rc;

                try {
                    cb = this.timeoutCallback.setExpiration(this.getQueryTimeout() * 1000);
                    /*
                     * The sqlite3_changes() function reports the changes for
                     * last DML statement that was executed and not the last
                     * statement executed, be it DDL/DML or otherwise.  So,
                     * we check the difference in total changes to see if
                     * the previous statement was actually an INSERT, UPDATE,
                     * or DELETE.
                     */
                    int initialChanges = Sqlite3.sqlite3_total_changes(this.conn.getHandle());

                    rc = Sqlite3.sqlite3_step(stmt.getPeer());
                    changeDiff = Sqlite3.sqlite3_total_changes(this.conn.getHandle()) - initialChanges;
                    if (cb != null && rc == Sqlite3.ReturnCodes.SQLITE_INTERRUPT.value()) {
                        throw new SQLTimeoutException("Query timeout reached");
                    }
                } finally {
                    closeQuietly(cb);
                }

                switch (Sqlite3.ReturnCodes.valueOf(rc)) {
                    case SQLITE_OK:
                    case SQLITE_DONE:
                        break;
                    default:
                        Sqlite3.checkOk(rc, this.conn.getHandle());
                        break;
                }
                this.replaceResultSet(null);
            }
        }
        finally {
            if (stmt != null) {
                Sqlite3.sqlite3_finalize(stmt);
            }
        }

        if (this.lastResult != null)
            this.lastUpdateCount = -1;
        else if (changeDiff > 0)
            this.lastUpdateCount = Sqlite3.sqlite3_changes(this.conn.getHandle());
        else
            this.lastUpdateCount = 0;

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

        // UsageWarning.log(Level.INFO, "Setting 'fetch size' will not affect performance with SQLite");
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
        String[] batchCopy = this.batchList.toArray(new String[this.batchList.size()]);
        int[] retval = new int[batchCopy.length];
        int index = 0;

        this.batchList.clear();

        for (String sql : batchCopy) {
            try {
                if (this.execute(sql)) {
                    ResultSet rs = null;

                    try {
                        rs = this.getResultSet();
                        if (rs.next()) {
                            LOGGER.log(Level.WARNING,
                                    "executeBatch used with a statement that is returning results -- {0}",
                                    new Object[] { sql });
                        }
                    } finally {
                        closeQuietly(rs);
                    }
                    retval[index] = SUCCESS_NO_INFO;
                }
                else {
                    retval[index] = this.lastUpdateCount;
                }
            }
            catch (SQLException e) {
                throw new BatchUpdateException(e);
            }
            index += 1;
        }

        return retval;
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

    public void closeOnCompletion() throws SQLException {
        requireOpened();

        this.closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        requireOpened();

        return this.closeOnCompletion;
    }

    @Override
    public String toString() {
        return this.lastQuery;
    }
}
