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

import org.bridj.BridJ;
import org.bridj.Pointer;
import org.bridj.util.Pair;
import org.sqlitejdbcng.bridj.Sqlite3;
import org.sqlitejdbcng.internal.TimeoutProgressCallback;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class SqlitePreparedStatement extends SqliteStatement implements PreparedStatement {
    private static final Integer INTEGER_ZERO = 0;
    private static final Integer INTEGER_ONE = 1;

    private final Pointer<Sqlite3.Statement> stmt;
    private final SqliteResultSetMetadata resultSetMetadata;
    private ParameterMetaData metadata;
    private final int paramCount;
    private final Object[] paramValues;
    private final int[] paramTypes;
    private final List<Pair<Object[], int[]>> batchParamList =
            new ArrayList<Pair<Object[], int[]>>();

    public SqlitePreparedStatement(SqliteConnection conn, Pointer<Sqlite3.Statement> stmt, String query)
            throws SQLException {
        super(conn);

        this.stmt = stmt;
        this.resultSetMetadata = new SqliteResultSetMetadata(this.stmt);
        this.lastQuery = query;
        this.paramCount = Sqlite3.sqlite3_bind_parameter_count(stmt);
        this.paramValues = new Object[this.paramCount];
        this.paramTypes = new int[this.paramCount];
        Arrays.fill(this.paramTypes, -1);
    }

    int checkParam(int index) {
        if (index < 1)
            throw new IllegalArgumentException("Parameter index must be greater than zero");
        if (index > this.paramCount)
            throw new IllegalArgumentException("Parameter index must be less than or equal to " + this.paramCount);

        return index;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!this.closed) {
            super.close();

            Sqlite3.sqlite3_finalize(this.stmt);
        }
    }

    void bindParameters(Object[] values, int[] types) throws SQLException {
        Sqlite3.checkOk(Sqlite3.sqlite3_reset(this.stmt));
        for (int lpc = 0; lpc < this.paramCount; lpc++) {
            int rc;

            switch (types[lpc]) {
                case -1:
                case Types.NULL:
                    rc = Sqlite3.sqlite3_bind_null(this.stmt, lpc + 1);
                    break;
                case Types.TINYINT:
                case Types.INTEGER:
                    rc = Sqlite3.sqlite3_bind_int(this.stmt, lpc + 1, ((Number) values[lpc]).intValue());
                    break;
                case Types.BIGINT:
                    rc = Sqlite3.sqlite3_bind_int64(this.stmt, lpc + 1, ((Number) values[lpc]).longValue());
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    rc = Sqlite3.sqlite3_bind_double(this.stmt, lpc + 1, ((Number) values[lpc]).doubleValue());
                    break;
                case Types.VARCHAR: {
                    String str = (String) values[lpc];
                    byte[] bits = str.getBytes(StandardCharsets.UTF_8);
                    Pointer<Byte> ptr = Sqlite3.sqlite3_malloc(bits.length);

                    ptr.setBytes(bits);
                    rc = Sqlite3.sqlite3_bind_text(
                            this.stmt,
                            lpc + 1,
                            ptr,
                            bits.length,
                            Sqlite3.SQLITE_FREE);
                    break;
                }
                case Types.VARBINARY: {
                    byte[] bytes = (byte[]) values[lpc];
                    Pointer<Byte> ptr = Sqlite3.sqlite3_malloc(bytes.length);

                    rc = Sqlite3.sqlite3_bind_blob(
                            this.stmt,
                            lpc + 1,
                            ptr,
                            bytes.length,
                            Sqlite3.SQLITE_FREE);
                    break;
                }
                case Types.BLOB: {
                    SqliteBlob sb = (SqliteBlob)values[lpc];
                    // TODO: remove this use of the buffer destructor
                    Sqlite3.BufferDestructorBase destructor = new Sqlite3.BufferDestructor(sb.getHandle());

                    BridJ.protectFromGC(destructor);
                    rc = Sqlite3.sqlite3_bind_blob(
                            this.stmt,
                            lpc + 1,
                            sb.getHandle(),
                            (int) sb.length(),
                            Pointer.pointerTo(destructor));
                    break;
                }
                default:
                    throw new SQLException("Internal error: unhandled SQL value -- (" +
                            types[lpc] + ") " + values[lpc], "XX000");
            }
            Sqlite3.checkOk(rc, this.conn.getHandle());
        }
    }

    @Override
    public void addBatch(String s) throws SQLException {
        throw new SQLNonTransientException("This operation is not supported on prepared statements", "42000");
    }

    @Override
    public void clearBatch() throws SQLException {
        requireOpened();

        this.batchParamList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        Pair[] batchCopy = this.batchParamList.toArray(new Pair[this.batchList.size()]);
        int[] retval = new int[batchCopy.length];
        SQLException lastException = null;
        int index = 0;

        this.batchParamList.clear();

        for (Pair pair : batchCopy) {
            try {
                this.clearWarnings();
                this.bindParameters((Object[])pair.getFirst(), (int[])pair.getSecond());
                this.executeUpdate();
                retval[index] = this.lastUpdateCount;
            }
            catch (SQLException e) {
                retval[index] = EXECUTE_FAILED;
                e.setNextException(lastException);
                lastException = e;
            }
            index += 1;
        }

        if (lastException != null) {
            throw new BatchUpdateException(lastException.getMessage(), lastException.getSQLState(),
                    lastException.getErrorCode(), retval, lastException);
        }

        return retval;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of executeQuery() to execute a prepared statement", "42000");
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of executeUpdate() to execute a prepared statement", "42000");
    }

    @Override
    public boolean execute(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of execute() to execute a prepared statement", "42000");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        requireOpened();

        this.clearWarnings();

        if (Sqlite3.sqlite3_column_count(this.stmt) == 0) {
            throw new SQLNonTransientException("SQL statement is not a query, use executeUpdate()", "42000");
        }

        this.bindParameters(this.paramValues, this.paramTypes);
        this.replaceResultSet(new SqliteResultSet(this, this.resultSetMetadata, this.stmt, this.maxRows));

        return this.lastResult;
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (this.execute()) {
            ResultSet rs = null;

            try {
                rs = this.getResultSet();
                rs.next();
            } finally {
                SqliteCommon.closeQuietly(rs);
            }
        }

        return this.lastUpdateCount;
    }

    @Override
    public void setNull(int i, int i2) throws SQLException {
        this.setObject(i, null, i2);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        this.setInt(i, b ? 1 : 0);
    }

    @Override
    public void setByte(int i, byte b) throws SQLException {
        this.setInt(i, b);
    }

    @Override
    public void setShort(int i, short s) throws SQLException {
        this.setInt(i, s);
    }

    @Override
    public void setInt(int i, int val) throws SQLException {
        this.setObject(i, val, Types.INTEGER);
    }

    @Override
    public void setLong(int i, long val) throws SQLException {
        this.setObject(i, val, Types.BIGINT);
    }

    @Override
    public void setFloat(int i, float val) throws SQLException {
        this.setObject(i, val, Types.FLOAT);
    }

    @Override
    public void setDouble(int i, double val) throws SQLException {
        this.setObject(i, val, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        this.setObject(i, bigDecimal, Types.DECIMAL);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        this.setObject(i, s, Types.VARCHAR);
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        this.setObject(i, bytes, Types.VARBINARY);
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        this.setDate(i, date, null);
    }

    @Override
    public void setTime(int i, Time time) throws SQLException {
        this.setTime(i, time, null);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        this.setTimestamp(i, timestamp, null);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearParameters() throws SQLException {
        requireOpened();

        Arrays.fill(this.paramValues, null);
        Arrays.fill(this.paramTypes, -1);
    }

    @Override
    public void setObject(int i, Object o, int targetSqlType) throws SQLException {
        this.setObject(i, o, targetSqlType, -1);
    }

    @Override
    public void setObject(int i, Object o) throws SQLException {
        if (o == null) {
            this.setNull(i, Types.OTHER);
            return;
        }

        int typeCode;

        if (o instanceof Long)
            typeCode = Types.BIGINT;
        else if (o instanceof byte[])
            typeCode = Types.VARBINARY;
        else if (o instanceof Blob)
            typeCode = Types.BLOB;
        else if (o instanceof Boolean)
            typeCode = Types.BOOLEAN;
        else if (o instanceof Byte)
            typeCode = Types.INTEGER;
        else if (o instanceof Character)
            typeCode = Types.CHAR;
        else if (o instanceof Clob)
            typeCode = Types.CLOB;
        else if (o instanceof Date)
            typeCode = Types.DATE;
        else if (o instanceof BigDecimal)
            typeCode = Types.DECIMAL;
        else if (o instanceof Double)
            typeCode = Types.DOUBLE;
        else if (o instanceof Float)
            typeCode = Types.FLOAT;
        else if (o instanceof Integer)
            typeCode = Types.INTEGER;
        else if (o instanceof Time)
            typeCode = Types.TIME;
        else if (o instanceof Timestamp)
            typeCode = Types.TIMESTAMP;
        else if (o instanceof String)
            typeCode = Types.VARCHAR;
        else if (o instanceof InputStream)
            typeCode = Types.BLOB;
        else
            throw new SQLFeatureNotSupportedException(
                    String.format("Conversion of object is not supported: %s", o.getClass()),
                    "0A000");

        this.setObject(i, o, typeCode);
    }

    @Override
    public boolean execute() throws SQLException {
        requireOpened();

        this.clearWarnings();

        this.bindParameters(this.paramValues, this.paramTypes);
        if (Sqlite3.sqlite3_column_count(this.stmt) != 0) {
            this.replaceResultSet(new SqliteResultSet(this, this.resultSetMetadata, this.stmt, this.maxRows));
        }
        else {
            TimeoutProgressCallback cb = null;
            int rc;

            try {
                cb = this.timeoutCallback.setExpiration(((long)this.getQueryTimeout()) * 1000L);
                rc = Sqlite3.sqlite3_step(stmt.getPeer());
                if (cb != null && rc == Sqlite3.ReturnCodes.SQLITE_INTERRUPT.value()) {
                    throw new SQLTimeoutException("Query timeout reached", "57000");
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

        if (this.lastResult != null) {
            this.lastUpdateCount = SUCCESS_NO_INFO;
        }
        else {
            this.lastUpdateCount = Sqlite3.sqlite3_changes(this.conn.getHandle());
        }

        return this.lastResult != null;
    }

    @Override
    public void addBatch() throws SQLException {
        requireOpened();

        this.batchParamList.add(new Pair<Object[], int[]>(this.paramValues, this.paramTypes));
    }

    @Override
    public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setRef(int i, Ref ref) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support REF values", "0A000");
    }

    @Override
    public void setBlob(int i, Blob blob) throws SQLException {
        this.setObject(i, blob, Types.BLOB);
    }

    @Override
    public void setClob(int i, Clob clob) throws SQLException {
        this.setObject(i, clob, Types.CLOB);
    }

    @Override
    public void setArray(int i, Array array) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support SQL arrays", "0A000");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        requireOpened();

        if (this.lastResult == null)
            return null;

        return this.lastResult.getMetaData();
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        if (date == null) {
            this.setNull(i, Types.DATE);
            return;
        }

        SimpleDateFormat format = DATE_FORMATTER.get();

        /* XXX */
        calendar = DEFAULT_CALENDAR.get();
        format.setCalendar(calendar);
        this.setString(i, format.format(date));
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        if (time == null) {
            this.setNull(i, Types.TIME);
            return;
        }

        SimpleDateFormat format = TIME_FORMATTER.get();

        if (calendar == null)
            calendar = DEFAULT_CALENDAR.get();
        format.setCalendar(calendar);
        this.setString(i, format.format(time));
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        if (timestamp == null) {
            this.setNull(i, Types.TIMESTAMP);
            return;
        }

        SimpleDateFormat format = TS_FORMATTER.get();

        if (calendar == null)
            calendar = DEFAULT_CALENDAR.get();
        format.setCalendar(calendar);
        this.setString(i, format.format(timestamp));
    }

    @Override
    public void setNull(int i, int i2, String s) throws SQLException {
        this.setNull(i, i2);
    }

    @Override
    public void setURL(int i, URL url) throws SQLException {
        this.setString(i, url.toString());
    }

    @Override
    public synchronized ParameterMetaData getParameterMetaData() throws SQLException {
        requireOpened();

        if (this.metadata == null)
            this.metadata = new SqliteParameterMetadata(this, this.stmt);

        return this.metadata;
    }

    @Override
    public void setRowId(int i, RowId rowId) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNString(int i, String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setObject(int i, Object o, int targetSqlType, int scaleOrLength) throws SQLException {
        requireOpened();
        checkParam(i);

        if (o == null) {
            this.paramTypes[i - 1] = Types.NULL;
            this.paramValues[i - 1] = null;
            return;
        }

        switch (targetSqlType) {
            case Types.ARRAY:
            case Types.DATALINK:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.SQLXML:
            case Types.STRUCT:
                throw new SQLFeatureNotSupportedException("SQLite does not support the given type", "0A000");
            case Types.NULL:
                this.paramValues[i - 1] = null;
                break;
            case Types.BIGINT:
                if (o instanceof Number) {
                    this.paramValues[i - 1] = o;
                }
                else {
                    throw new SQLNonTransientException(
                            "Conversion to BIGINT not supported for value -- " + o, "22000");
                }
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
                if (o instanceof byte[]) {
                    this.paramValues[i - 1] = o;
                    targetSqlType = Types.VARBINARY;
                }
                else if (o instanceof Blob) {
                    this.paramValues[i - 1] = o;
                    targetSqlType = Types.BLOB;
                }
                else
                    throw new SQLNonTransientException(
                            "Conversion to BLOB not supported for value -- " + o, "22000");
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                if (o instanceof Boolean) {
                    this.paramValues[i - 1] = ((Boolean)o).booleanValue() ? INTEGER_ONE : INTEGER_ZERO;
                    targetSqlType = Types.INTEGER;
                }
                else if (o instanceof Number)
                    this.paramValues[i - 1] = o;
                else
                    throw new SQLNonTransientException("Conversion to boolean not supported for value -- " + o, "22000");
                break;
            case Types.CHAR:
                if (o instanceof Character) {
                    this.paramValues[i - 1] = o.toString();
                    targetSqlType = Types.VARCHAR;
                }
                else
                    throw new SQLNonTransientException(
                            "Conversion to CHAR not supported for value -- " + o, "22000");
                break;
            case Types.CLOB:
                if (o instanceof Clob)
                    this.paramValues[i - 1] = o;
                else
                    throw new SQLNonTransientException(
                            "Conversion to CLOB not support for value -- " + o, "22000");
                break;
            case Types.DATE:
                if (o instanceof Date)
                    this.setDate(i, (Date)o);
                else
                    throw new SQLNonTransientException(
                            "Conversion to DATE not support for value -- " + o, "22000");
                break;
            case Types.DECIMAL:
                if (o instanceof BigDecimal) {
                    this.paramValues[i - 1] = ((BigDecimal)o).toPlainString();
                    targetSqlType = Types.VARCHAR;
                }
                else if (o instanceof Number) {
                    this.paramValues[i - 1] = o;
                    targetSqlType = Types.BIGINT;
                }
                else
                    throw new SQLNonTransientException(
                            "Conversion to DECIMAL not support for value -- " + o, "22000");
                break;
            case Types.FLOAT:
                if (o instanceof Number)
                    this.paramValues[i - 1] = o;
                else
                    throw new SQLNonTransientException(
                            "Conversion to FLOAT not support for value -- " + o, "22000");
                break;
            case Types.REAL:
            case Types.DOUBLE:
                if (o instanceof Number)
                    this.paramValues[i - 1] = o;
                else
                    throw new SQLNonTransientException(
                            "Conversion to REAL not support for value -- " + o, "22000");
                break;
            case Types.INTEGER:
            case Types.TINYINT:
                if (o instanceof Number)
                    this.paramValues[i - 1] = o;
                else
                    throw new SQLNonTransientException(
                            "Conversion to INTEGER not support for value -- " + o, "22000");
                break;
            case Types.NUMERIC:
                this.paramValues[i - 1] = o.toString();
                targetSqlType = Types.VARCHAR;
                break;
            case Types.TIME:
                if (o instanceof Time)
                    this.setTime(i, (Time)o);
                else
                    throw new SQLNonTransientException(
                            "Conversion to TIME not support for value -- " + o, "22000");
                break;
            case Types.TIMESTAMP:
                if (o instanceof Timestamp)
                    this.setTimestamp(i, (Timestamp)o);
                else if (o instanceof Date)
                    this.setTimestamp(i, new Timestamp(((Date)o).getTime()));
                else
                    throw new SQLNonTransientException(
                            "Conversion to TIMESTAMP not support for value -- " + o, "22000");
                break;
            case Types.VARCHAR:
                this.paramValues[i - 1] = o.toString();
                break;
            default:
                throw new SQLFeatureNotSupportedException("SQLite does not support the given type", "0A000");
        }
        this.paramTypes[i - 1] = targetSqlType;
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
