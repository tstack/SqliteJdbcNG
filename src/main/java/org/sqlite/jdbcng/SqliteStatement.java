package org.sqlite.jdbcng;

import org.bridj.Pointer;
import org.sqlite.jdbcng.bridj.Sqlite3;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqliteStatement implements Statement {
    protected final SqliteConnection conn;
    protected final List<String> batchList = new ArrayList<String>();
    protected SqliteResultSet lastResult;

    public SqliteStatement(SqliteConnection conn) {
        this.conn = conn;
    }

    Pointer<Sqlite3.Statement> requireAccess(Pointer<Sqlite3.Statement> stmt) throws SQLException {
        if (this.conn.isReadOnly() && Sqlite3.sqlite3_stmt_readonly(stmt) == 0) {
            Sqlite3.sqlite3_finalize(stmt);
            throw new SQLNonTransientException(
                    "Connection is in read-only mode, but statement is not read-only");
        }

        return stmt;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL));

        this.lastResult = new SqliteResultSet(this, requireAccess(stmt_out.get()));

        return this.lastResult;
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        Pointer<Pointer<Sqlite3.Statement>> stmt_out = Pointer.allocatePointer(Sqlite3.Statement.class);

        if (this.conn.isReadOnly())
            throw new SQLNonTransientException(
                    "Updates cannot be performed while the connection is in read-only mode.");

        Sqlite3.checkOk(Sqlite3.sqlite3_prepare_v2(this.conn.getHandle(),
                Pointer.pointerToCString(s), -1, stmt_out, Pointer.NULL));

        try {
            if (Sqlite3.sqlite3_stmt_readonly(stmt_out.get()) != 0)
                throw new SQLNonTransientException("SQL statement does not contain an update");

            int rc = Sqlite3.sqlite3_step(stmt_out.get());

            switch (Sqlite3.ReturnCodes.valueOf(rc)) {
                case SQLITE_OK:
                case SQLITE_DONE:
                    break;
                default:
                    Sqlite3.checkOk(rc);
                    break;
            }
        }
        finally {
            Sqlite3.sqlite3_finalize(stmt_out.get());
        }

        return Sqlite3.sqlite3_changes(this.conn.getHandle());
    }

    @Override
    public void close() throws SQLException {
        if (this.lastResult != null) {
            Pointer<Sqlite3.Statement> stmt = this.lastResult.getHandle();

            this.lastResult.close();
            this.lastResult = null;
            Sqlite3.sqlite3_finalize(stmt);
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
    public SQLWarning getWarnings() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearWarnings() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCursorName(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean execute(String s) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addBatch(String s) throws SQLException {
        this.batchList.add(s);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.batchList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
        this.close();
    }
}
