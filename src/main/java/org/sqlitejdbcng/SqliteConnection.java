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
import org.sqlitejdbcng.internal.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.sqlitejdbcng.internal.EscapeParser.transform;

public class SqliteConnection extends SqliteCommon implements Connection {
    private static final Logger LOGGER = Logger.getLogger(SqliteConnection.class.getName());

    private static final SQLPermission CALL_ABORT_PERM = new SQLPermission("callAbort");

    private static final Sqlite3.AuthCallbackBase RO_AUTHORIZER = new Sqlite3.AuthCallbackBase() {
        @Override
        public int apply(Pointer<Void> context, int actionCode, Pointer<Byte> arg1, Pointer<Byte> arg2, Pointer<Byte> arg3, Pointer<Byte> arg4) {
            Sqlite3.ActionCode acEnum = Sqlite3.ActionCode.valueOf(actionCode);

            if (acEnum == null)
                return Sqlite3.AuthResult.SQLITE_DENY.value();

            switch (acEnum) {
                case SQLITE_PRAGMA:
                    if (arg2 == null) {
                        return Sqlite3.AuthResult.SQLITE_OK.value();
                    }
                    return Sqlite3.AuthResult.SQLITE_DENY.value();
                case SQLITE_ATTACH:
                case SQLITE_DETACH:
                case SQLITE_READ:
                case SQLITE_SELECT:
                case SQLITE_FUNCTION:
                    return Sqlite3.AuthResult.SQLITE_OK.value();
                default:
                    return Sqlite3.AuthResult.SQLITE_DENY.value();
            }
        }
    };

    private static final Map<String, EscapeHandler> HANDLER_MAP =
            new HashMap<String, EscapeHandler>();

    static {
        EscapeHandler passThruPair = new PassthruEscapeHandler(true);
        EscapeHandler passThruArg = new PassthruEscapeHandler(false);

        HANDLER_MAP.put("limit", passThruPair);
        HANDLER_MAP.put("escape", passThruPair);
        HANDLER_MAP.put("fn", new FunctionEscapeHandler());
        HANDLER_MAP.put("d", passThruArg);
        HANDLER_MAP.put("t", passThruArg);
        HANDLER_MAP.put("ts", passThruArg);
        HANDLER_MAP.put("oj", passThruArg);
    }

    private final String url;
    private final Pointer<Sqlite3.Sqlite3Db> db;
    private final Properties properties;
    private final List<WeakRefWithEquals<Statement>> statements =
            new ArrayList<WeakRefWithEquals<Statement>>();
    private SqliteDatabaseMetadata metadata;
    private boolean readOnly;
    private final CloseNotifier closer = new CloseNotifier();
    private boolean halfClosed;
    private int savepointId;
    private int progressStep = 100;
    private SqliteConnectionProgressCallback callback;

    public SqliteConnection(String url, Properties properties) throws SQLException {
        Pointer<Pointer<Sqlite3.Sqlite3Db>> db_out = Pointer.allocatePointer(Sqlite3.Sqlite3Db.class);
        SqliteUrl sqliteUrl = new SqliteUrl(url);
        int rc = Sqlite3.sqlite3_open_v2(
                Pointer.pointerToCString(sqliteUrl.getPath()),
                db_out,
                Sqlite3.OpenFlag.SQLITE_OPEN_READWRITE.intValue() |
                        Sqlite3.OpenFlag.SQLITE_OPEN_CREATE.intValue() |
                        Sqlite3.OpenFlag.SQLITE_OPEN_URI.intValue(),
                null);

        try {
            Sqlite3.sqlite3_enable_load_extension(db_out.get(), 1);
        }
        catch (UnsatisfiedLinkError e) {
            LOGGER.warning("sqlite3_enable_load_extension function is not available");
        }
        this.url = url;
        this.db = Sqlite3.withDbReleaser(db_out.get());
        this.properties = properties;

        Sqlite3.checkOk(rc);

        /*
         * Do an initial query to make sure the database is valid.  If there
         * is something wrong with it, it will throw a SQLITE_NOTADB error.
         */
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.createStatement();
            rs = stmt.executeQuery("PRAGMA database_list");
            rs.next();
        } finally {
            closeQuietly(rs);
            closeQuietly(stmt);
        }
    }

    synchronized int nextSavepointId() {
        return this.savepointId++;
    }

    void requireOpened() throws SQLException {
        if (this.isClosed()) {
            throw new SQLNonTransientException("Database is closed for business");
        }
    }

    void requireNoTransaction() throws SQLException {
        if (!this.getAutoCommit()) {
            throw new SQLNonTransientException("Operation cannot be performed in the middle of a transaction");
        }
    }

    private void requireTransaction() throws SQLException {
        if (this.getAutoCommit()) {
            throw new SQLNonTransientException("Operation cannot be performed while in auto-commit mode");
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

        Statement stmt = null;
        try {
            stmt = this.createStatement();
            stmt.executeUpdate(sql);
        } finally {
            closeQuietly(stmt);
        }
    }

    public void setProgressStep(int step) {
        this.progressStep = step;
    }

    public void pushCallback(SqliteConnectionProgressCallback callback) throws SQLException {
        requireOpened();

        if (callback == null)
            throw new SQLNonTransientException("Callback cannot be null");

        callback.setOther(this.callback);
        this.callback = callback;
        Sqlite3.sqlite3_progress_handler(
                this.db,
                this.progressStep,
                Pointer.pointerTo((Sqlite3.ProgressCallbackBase) this.callback),
                null);
    }

    public void popCallback() throws SQLException {
        requireOpened();

        if (this.callback == null)
            throw new SQLNonTransientException("Callback stack is empty");

        this.callback = this.callback.getOther();
        Sqlite3.sqlite3_progress_handler(
                this.db,
                this.progressStep,
                Pointer.pointerTo((Sqlite3.ProgressCallbackBase) this.callback),
                null);
    }

    public String getURL() {
        return this.url;
    }

    public Pointer<Sqlite3.Sqlite3Db> getHandle() {
        return this.db;
    }

    private <T extends Statement> T trackStatement(T stmt) {
        synchronized (this.statements) {
            this.statements.add(new WeakRefWithEquals<Statement>(stmt));
        }

        return stmt;
    }

    void statementClosed(Statement stmt) {
        synchronized (this.statements) {
            this.statements.remove(new WeakRefWithEquals<Statement>(stmt));
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return this.trackStatement(this.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        return this.trackStatement(this.prepareStatement(s,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support stored procedures");
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        String retval = transform(s, HANDLER_MAP);

        return retval;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        boolean currentValue = this.getAutoCommit();

        if (currentValue != autoCommit) {
            if (autoCommit) {
                this.executeCanned("COMMIT");
            }
            else {
                this.executeCanned("BEGIN");
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        requireOpened();

        return Sqlite3.sqlite3_get_autocommit(this.db) != 0;
    }

    @Override
    public void commit() throws SQLException {
        this.executeCanned("COMMIT");
        this.setAutoCommit(false);
    }

    @Override
    public void rollback() throws SQLException {
        this.executeCanned("ROLLBACK");
        this.setAutoCommit(false);
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!this.closer.isClosed()) {
            synchronized (this.statements) {
                /*
                 * JDBC Spec 9.4.4.1: All Statement objects created from a given
                 * Connection object will be closed when the close method for
                 * the Connection object is called.
                 */

                while (!this.statements.isEmpty()) {
                    WeakRefWithEquals<Statement> stmtRef = this.statements.remove(this.statements.size() - 1);
                    Statement stmt = stmtRef.get();

                    if (stmt == null)
                        continue;

                    if (!stmt.isClosed()) {
                        LOGGER.log(Level.WARNING,
                                "Statement was not explicitly closed -- {0}",
                                new Object[] { stmt.toString() });
                        stmt.close();
                    }
                }
            }

            this.db.release();
            this.closer.close();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.halfClosed || this.closer.isClosed();
    }

    @Override
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        requireOpened();

        if (this.metadata == null)
            this.metadata = new SqliteDatabaseMetadata(this);

        return this.metadata;
    }

    @Override
    public synchronized void setReadOnly(boolean b) throws SQLException {
        requireOpened();
        requireNoTransaction();

        if (b != this.readOnly) {
            if (b) {
                Sqlite3.checkOk(Sqlite3.sqlite3_set_authorizer(this.db, Pointer.pointerTo(RO_AUTHORIZER), null),
                        this.db);
            }
            else {
                Sqlite3.sqlite3_set_authorizer(this.db, null, null);
            }
            this.readOnly = b;
        }
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
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.createStatement();
            rs = stmt.executeQuery("PRAGMA read_uncommitted");

            rs.next();
            if (rs.getBoolean(1))
                return TRANSACTION_READ_UNCOMMITTED;
            else
                return TRANSACTION_SERIALIZABLE;
        } finally {
            closeQuietly(rs);
            closeQuietly(stmt);
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

    private void execSavepointStatement(String sql, SqliteSavepoint sp) throws SQLException {
        requireOpened();
        requireTransaction();

        String fullSql = Sqlite3.mprintf(sql, sp.getSqliteName());

        Statement stmt = null;
        try {
            stmt = this.createStatement();
            stmt.executeUpdate(fullSql);
        } finally {
            closeQuietly(stmt);
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        SqliteSavepoint retval = new SqliteSavepoint(this.nextSavepointId());

        execSavepointStatement("SAVEPOINT %q", retval);

        return retval;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        SqliteSavepoint retval = new SqliteSavepoint(name);

        execSavepointStatement("SAVEPOINT %q", retval);

        return retval;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        execSavepointStatement("ROLLBACK TO %q", (SqliteSavepoint)savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        execSavepointStatement("RELEASE SAVEPOINT %q", (SqliteSavepoint)savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability)
            throws SQLException {
        requireOpened();
        this.clearWarnings();
        requireResultSetType(resultSetType, resultSetConcurrency, resultSetHoldability);

        return new SqliteStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String s,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        requireOpened();
        this.clearWarnings();
        requireResultSetType(resultSetType, resultSetConcurrency, resultSetHoldability);

        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.db,
                Pointer.pointerToCString(this.nativeSQL(s)), -1, stmt_out, Pointer.NULL),
                this.db);

        return new SqlitePreparedStatement(this, stmt_out.get(), s);
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
        return !this.isClosed();
    }

    @Override
    public void setClientInfo(String k, String v) throws SQLClientInfoException {
        try {
            requireOpened();

            throw new SQLFeatureNotSupportedException("SQLite does not support client info");
        }
        catch (SQLException e) {
            throw new SQLClientInfoException(
                    Collections.singletonMap(k, ClientInfoStatus.REASON_UNKNOWN_PROPERTY),
                    e);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            requireOpened();

            throw new SQLFeatureNotSupportedException("SQLite does not support client info");
        }
        catch (SQLException e) {
            Map<String, ClientInfoStatus> map = new HashMap<String, ClientInfoStatus>();

            for (Object key : properties.keySet()) {
                map.put((String) key, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            }
            throw new SQLClientInfoException(map, e);
        }
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        requireOpened();

        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        requireOpened();

        return new Properties();
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support arrays");
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support structs");
    }

    public void setSchema(String schema) throws SQLException {
        requireOpened();
    }

    public String getSchema() throws SQLException {
        requireOpened();

        return "";
    }

    public synchronized void abort(Executor executor) throws SQLException {
        SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(CALL_ABORT_PERM);
        }
        if (!this.isClosed()) {
            Sqlite3.sqlite3_interrupt(this.db);
            this.halfClosed = true;
            if (executor != null) {
                executor.execute(this.closer);
            }
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite is a local-only database");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite is a local-only database");
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        if (!this.isClosed()) {
            LOGGER.log(Level.WARNING,
                    "SQLite database connection was not explicitly closed -- {0}",
                    this.url);

            this.close();
        }
    }
}
