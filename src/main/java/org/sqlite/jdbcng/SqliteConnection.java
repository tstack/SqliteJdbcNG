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

import java.lang.ref.WeakReference;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqliteConnection extends SqliteCommon implements Connection {
    private static final Logger LOGGER = Logger.getLogger(SqliteConnection.class.getName());

    private final String url;
    private final Pointer<Sqlite3.Sqlite3Db> db;
    private final Properties properties;
    private final List<WeakReference<Statement>> statements = new ArrayList<WeakReference<Statement>>();
    private SqliteDatabaseMetadata metadata;
    private boolean readOnly;
    private boolean closed;

    public SqliteConnection(String url, Properties properties) throws SQLException {
        Pointer<Pointer<Sqlite3.Sqlite3Db>> db_out = Pointer.allocatePointer(Sqlite3.Sqlite3Db.class);
        SqliteUrl sqliteUrl = new SqliteUrl(url);
        int rc = Sqlite3.sqlite3_open(Pointer.pointerToCString(sqliteUrl.getPath()), db_out);

        this.url = url;
        this.db = Sqlite3.withDbReleaser(db_out.get());
        this.properties = properties;

        Sqlite3.checkOk(rc);

        /*
         * Do an initial query to make sure the database is valid.  If there
         * is something wrong with it, it will throw a SQLITE_NOTADB error.
         */
        this.executeCanned("PRAGMA database_list");
    }

    void requireOpened() throws SQLException {
        if (this.closed) {
            throw new SQLNonTransientException("Database is closed for business");
        }
    }

    void requireResultSetType(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY)
            this.addWarning(new SQLWarning("SQLite only supports TYPE_FORWARD_ONLY result sets"));
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
            this.addWarning(new SQLWarning("SQLite only supports CONCUR_READ_ONLY result sets"));
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            this.addWarning(new SQLWarning("SQLite only supports CLOSE_CURSORS_AT_COMMIT result sets"));
    }

    void executeCanned(String sql) throws SQLException {
        requireOpened();

        try (Statement stmt = this.createStatement()) {
            stmt.execute(sql);
        }
    }

    public String getURL() {
        return this.url;
    }

    public Pointer<Sqlite3.Sqlite3Db> getHandle() {
        return this.db;
    }

    private <T extends Statement> T trackStatement(T stmt) {
        synchronized (this.statements) {
            this.statements.add(new WeakReference<Statement>(stmt));
        }

        return stmt;
    }

    void statementClosed(Statement stmt) {
        synchronized (this.statements) {
            this.statements.remove(new WeakReference<Statement>(stmt));
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return this.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        return this.prepareStatement(s,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support stored procedures");
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        requireOpened();

        if (!autoCommit) {
            this.executeCanned("BEGIN");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        requireOpened();

        return Sqlite3.sqlite3_get_autocommit(this.db) != 0;
    }

    @Override
    public void commit() throws SQLException {
        boolean autoCommit = this.getAutoCommit();

        try {
            this.executeCanned("COMMIT");
        }
        finally {
            this.setAutoCommit(autoCommit);
        }
    }

    @Override
    public void rollback() throws SQLException {
        boolean autoCommit = this.getAutoCommit();

        try {
            this.executeCanned("ROLLBACK");
        }
        finally {
            this.setAutoCommit(autoCommit);
        }
    }

    @Override
    public void close() throws SQLException {
        if (!this.closed) {
            /*
             * JDBC Spec 9.4.4.1: All Statement objects created from a given
             * Connection object will be closed when the close method for
             * the Connection object is called.
             */
            synchronized (this.statements) {
                for (WeakReference<Statement> stmtRef : this.statements) {
                    Statement stmt = stmtRef.get();

                    if (stmt == null)
                        continue;

                    if (!stmt.isClosed()) {
                        LOGGER.log(Level.WARNING,
                                "Statement was not explicitly closed -- {0}",
                                new Object[] { ((SqliteStatement)stmt).getLastQuery() });
                        stmt.close();
                    }
                }
            }

            this.db.release();
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        requireOpened();

        if (this.metadata == null)
            this.metadata = new SqliteDatabaseMetadata(this);

        return this.metadata;
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        requireOpened();

        this.readOnly = true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        requireOpened();

        return this.readOnly;
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        requireOpened();

        String msg = String.format(
                "setCatalog(%s) is not supported by SQLite, use fully qualified names in SQL statements",
                s);

        addWarning(new SQLWarning(msg));
    }

    @Override
    public String getCatalog() throws SQLException {
        requireOpened();

        return "";
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        requireOpened();

        switch (level) {
            case TRANSACTION_SERIALIZABLE:
                this.executeCanned("PRAGMA read_uncommitted = false");
                break;
            case TRANSACTION_READ_UNCOMMITTED:
                this.executeCanned("PRAGMA read_uncommitted = true");
                break;
            default:
                throw new SQLFeatureNotSupportedException(
                        "SQLite only supports TRANSACTION_SERIALIZABLE and TRANSACTION_READ_UNCOMMITTED");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        requireOpened();

        try (Statement stmt = this.createStatement()) {
            ResultSet rs = stmt.executeQuery("PRAGMA read_uncommitted");

            rs.next();
            if (rs.getBoolean(1))
                return TRANSACTION_READ_UNCOMMITTED;
            else
                return TRANSACTION_SERIALIZABLE;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return this.prepareStatement(s, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
        return this.prepareCall(s);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support user-defined types");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support user-defined types");
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        requireOpened();

        if (i != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            throw new SQLFeatureNotSupportedException("SQLite only supports CLOSE_CURSORS_AT_COMMIT");
    }

    @Override
    public int getHoldability() throws SQLException {
        requireOpened();

        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability)
            throws SQLException {
        requireOpened();
        requireResultSetType(resultSetType, resultSetConcurrency, resultSetHoldability);

        return this.trackStatement(new SqliteStatement(this));
    }

    @Override
    public PreparedStatement prepareStatement(String s,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        requireOpened();
        requireResultSetType(resultSetType, resultSetConcurrency, resultSetHoldability);

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.db,
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL));

        return this.trackStatement(new SqlitePreparedStatement(this, stmt_out.get()));
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
        return this.prepareCall(s);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");

        return this.prepareStatement(s);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support returning generated keys");
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob createBlob() throws SQLException {
        requireOpened();

        return new SqliteBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support SQLXML");
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return !this.closed;
    }

    @Override
    public void setClientInfo(String s, String s2) throws SQLClientInfoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support arrays");
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support structs");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        requireOpened();
    }

    @Override
    public String getSchema() throws SQLException {
        requireOpened();

        return "";
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
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
        if (!this.isClosed()) {
            LOGGER.log(Level.WARNING,
                    "SQLite database connection was not explicitly closed -- {0}",
                    this.url);

            this.close();
        }
    }
}
