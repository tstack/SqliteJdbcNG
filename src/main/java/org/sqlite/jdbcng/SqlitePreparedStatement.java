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

import org.bridj.BridJ;
import org.bridj.Pointer;
import org.sqlite.jdbcng.bridj.Sqlite3;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class SqlitePreparedStatement extends SqliteStatement implements PreparedStatement {
    private final Pointer<Sqlite3.Statement> stmt;
    private ParameterMetaData metadata;
    private final int paramCount;

    public SqlitePreparedStatement(SqliteConnection conn, Pointer<Sqlite3.Statement> stmt)
            throws SQLException {
        super(conn);

        this.stmt = requireAccess(stmt);
        this.paramCount = Sqlite3.sqlite3_bind_parameter_count(stmt);
        this.lastResult = null;
    }

    int checkParam(int index) {
        if (index < 1)
            throw new IllegalArgumentException("Parameter index must be greater than zero");
        if (index > this.paramCount)
            throw new IllegalArgumentException("Parameter index must be less than or equal to " + this.paramCount);

        return index;
    }

    void requireClosedResult() throws SQLException {
        if (this.lastResult != null && this.lastResult.isActive()) {
            throw new SQLNonTransientException("Previous result set for statement must be closed before parameters can be rebound");
        }
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of executeQuery() to execute a prepared statement");
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of executeUpdate() to execute a prepared statement");
    }

    @Override
    public boolean execute(String s) throws SQLException {
        throw new SQLNonTransientException("Use the no-argument version of execute() to execute a prepared statement");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (this.lastResult != null)
            this.lastResult.close();

        this.clearWarnings();

        this.lastResult = new SqliteResultSet(this, this.stmt);
        return this.lastResult;
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (Sqlite3.sqlite3_stmt_readonly(this.stmt) != 0)
            throw new SQLNonTransientException("SQL statement does not contain an update");

        Sqlite3.sqlite3_reset(this.stmt);

        this.clearWarnings();

        int rc = Sqlite3.sqlite3_step(this.stmt);

        switch (Sqlite3.ReturnCodes.valueOf(rc)) {
            case SQLITE_OK:
            case SQLITE_DONE:
                break;
            default:
                Sqlite3.checkOk(rc, this.conn.getHandle());
                break;
        }

        return Sqlite3.sqlite3_changes(this.conn.getHandle());
    }

    @Override
    public void setNull(int i, int i2) throws SQLException {
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_null(this.stmt, checkParam(i)),
                this.conn.getHandle());
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
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_int(this.stmt, checkParam(i), val),
                this.conn.getHandle());
    }

    @Override
    public void setLong(int i, long val) throws SQLException {
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_int64(this.stmt, checkParam(i), val),
                this.conn.getHandle());
    }

    @Override
    public void setFloat(int i, float val) throws SQLException {
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_double(this.stmt, checkParam(i), val),
                this.conn.getHandle());
    }

    @Override
    public void setDouble(int i, double val) throws SQLException {
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_double(this.stmt, checkParam(i), val),
                this.conn.getHandle());
    }

    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        requireClosedResult();

        this.setString(i, bigDecimal.toString());
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        requireClosedResult();

        Sqlite3.checkOk(Sqlite3.sqlite3_bind_text(
                this.stmt, checkParam(i), Pointer.pointerToCString(s), -1, Sqlite3.SQLITE_TRANSIENT),
                this.conn.getHandle());
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        requireClosedResult();

        Pointer<Byte> ptr = Pointer.pointerToBytes(bytes);
        Sqlite3.BufferDestructorBase destructor = new Sqlite3.BufferDestructor(ptr);

        BridJ.protectFromGC(destructor);
        Sqlite3.checkOk(Sqlite3.sqlite3_bind_blob(
                this.stmt,
                i,
                ptr,
                bytes.length,
                Pointer.pointerTo(destructor)),
                this.conn.getHandle());
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
        requireClosedResult();

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {
        requireClosedResult();

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
        requireClosedResult();

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearParameters() throws SQLException {
        Sqlite3.checkOk(Sqlite3.sqlite3_clear_bindings(this.stmt), this.conn.getHandle());
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

        if (o instanceof Long)
            this.setLong(i, (Long) o);
        else if (o instanceof byte[])
            this.setBytes(i, (byte[]) o);
        else if (o instanceof Blob)
            this.setBlob(i, (Blob) o);
        else if (o instanceof Boolean)
            this.setBoolean(i, (Boolean) o);
        else if (o instanceof Byte)
            this.setByte(i, (Byte)o);
        else if (o instanceof Character)
            this.setObject(i, o, Types.CHAR);
        else if (o instanceof Clob)
            this.setClob(i, (Clob) o);
        else if (o instanceof Date)
            this.setDate(i, (Date) o);
        else if (o instanceof BigDecimal)
            this.setBigDecimal(i, (BigDecimal) o);
        else if (o instanceof Double)
            this.setDouble(i, (Double) o);
        else if (o instanceof Float)
            this.setFloat(i, (Float) o);
        else if (o instanceof Integer)
            this.setInt(i, (Integer)o);
        else if (o instanceof Time)
            this.setTime(i, (Time)o);
        else if (o instanceof Timestamp)
            this.setTimestamp(i, (Timestamp)o);
        else if (o instanceof String)
            this.setString(i, (String)o);
        else if (o instanceof InputStream)
            this.setBinaryStream(i, (InputStream)o);
        else
            throw new SQLFeatureNotSupportedException("");
    }

    @Override
    public boolean execute() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addBatch() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {
        requireClosedResult();

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setRef(int i, Ref ref) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support REF values");
    }

    @Override
    public void setBlob(int i, Blob blob) throws SQLException {
        requireClosedResult();

        SqliteBlob sb = (SqliteBlob)blob;
        Sqlite3.BufferDestructorBase destructor = new Sqlite3.BufferDestructor(sb.getHandle());

        BridJ.protectFromGC(destructor);
        Sqlite3.checkOk(Sqlite3.sqlite3_bind_blob(
                this.stmt,
                checkParam(i),
                sb.getHandle(),
                (int) sb.length(),
                Pointer.pointerTo(destructor)),
                this.conn.getHandle());
    }

    @Override
    public void setClob(int i, Clob clob) throws SQLException {
        requireClosedResult();

        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setArray(int i, Array array) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support SQL arrays");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.lastResult.getMetaData();
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        requireClosedResult();

        if (date == null) {
            this.setNull(i, Types.DATE);
            return;
        }

        SimpleDateFormat format = DATE_FORMATTER.get();

        if (calendar == null)
            calendar = DEFAULT_CALENDAR.get();
        format.setCalendar(calendar);
        this.setString(i, format.format(date));
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        requireClosedResult();

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
        requireClosedResult();

        if (timestamp == null) {
            this.setNull(i, Types.TIME);
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
        if (o == null) {
            this.setNull(i, targetSqlType);
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
                throw new SQLFeatureNotSupportedException("SQLite does not support the given type");
            case Types.NULL:
                this.setNull(i, targetSqlType);
                break;
            case Types.BIGINT:
                if (o instanceof Number)
                    this.setLong(i, ((Number)o).longValue());
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
                if (o instanceof InputStream)
                    this.setBinaryStream(i, (InputStream)o, scaleOrLength);
                else if (o instanceof byte[])
                    this.setBytes(i, (byte[])o);
                else if (o instanceof Blob)
                    this.setBlob(i, (Blob)o);
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                if (o instanceof Boolean)
                    this.setBoolean(i, ((Boolean)o));
                else if (o instanceof Number)
                    this.setBoolean(i, ((Number)o).intValue() != 0);
                else
                    throw new SQLNonTransientException("Conversion to boolean not support for value -- " + o);
                break;
            case Types.CHAR:
                if (o instanceof Character)
                    this.setString(i, o.toString());
                else
                    throw new SQLNonTransientException("Conversion to boolean not support for value -- " + o);
                break;
            case Types.CLOB:
                if (o instanceof InputStream)
                    this.setBinaryStream(i, (InputStream) o, scaleOrLength);
                else if (o instanceof byte[])
                    this.setBytes(i, (byte[])o);
                else if (o instanceof Clob)
                    this.setClob(i, (Clob) o);
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.DATE:
                if (o instanceof Date)
                    this.setDate(i, (Date)o);
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.DECIMAL:
                if (o instanceof BigDecimal)
                    this.setBigDecimal(i, (BigDecimal)o);
                else if (o instanceof Number)
                    this.setDouble(i, ((Number) o).doubleValue());
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.FLOAT:
                if (o instanceof Number)
                    this.setFloat(i, ((Number)o).floatValue());
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.REAL:
            case Types.DOUBLE:
                if (o instanceof Number)
                    this.setDouble(i, ((Number)o).doubleValue());
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.INTEGER:
            case Types.TINYINT:
                if (o instanceof Number)
                    this.setInt(i, ((Number)o).intValue());
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.NUMERIC:
                this.setString(i, o.toString());
                break;
            case Types.TIME:
                if (o instanceof Time)
                    this.setTime(i, (Time)o);
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.TIMESTAMP:
                if (o instanceof Timestamp)
                    this.setTimestamp(i, (Timestamp)o);
                else if (o instanceof Date)
                    this.setTimestamp(i, new Timestamp(((Date)o).getTime()));
                else
                    throw new SQLNonTransientException("Conversion to long not support for value -- " + o);
                break;
            case Types.VARCHAR:
                this.setString(i, o.toString());
                break;
            default:
                throw new SQLFeatureNotSupportedException("SQLite does not support the given type");
        }
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
