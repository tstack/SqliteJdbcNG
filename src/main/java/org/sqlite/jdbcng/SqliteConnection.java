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

public class SqliteConnection implements Connection {
    private final String url;
    private final Pointer<Sqlite3.Sqlite3Db> db;
    private final Properties properties;
    private final List<WeakReference> statements = new ArrayList<WeakReference>();
    private SqliteDatabaseMetadata metadata;
    private boolean closed;

    public SqliteConnection(String url, Properties properties) throws SQLException {
        Pointer<Pointer<Sqlite3.Sqlite3Db>> db_out = Pointer.allocatePointer(Sqlite3.Sqlite3Db.class);
        SqliteUrl sqliteUrl = new SqliteUrl(url);
        int rc = Sqlite3.sqlite3_open(Pointer.pointerToCString(sqliteUrl.getPath()), db_out);

        Sqlite3.checkOk(rc);

        this.url = url;
        this.db = db_out.get();
        this.properties = properties;
    }

    public String getURL() {
        return this.url;
    }

    public Pointer<Sqlite3.Sqlite3Db> getHandle() {
        return this.db;
    }

    @Override
    public Statement createStatement() throws SQLException {
        Statement retval = new SqliteStatement(this);

        synchronized (this.statements) {
            this.statements.add(new WeakReference(retval));
        }

        return retval;
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.db,
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL));

        return new SqlitePreparedStatement(this, stmt_out.get());
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
    public void setAutoCommit(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commit() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
                        continue;;

                    stmt.close();
                }
            }
            this.closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        if (this.metadata == null)
            this.metadata = new SqliteDatabaseMetadata(this);

        return this.metadata;
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearWarnings() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Statement createStatement(int i, int i2) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i2) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
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
    public Statement createStatement(int i, int i2, int i3) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i2, int i3) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getSchema() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
}
