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
import java.util.Calendar;

public class SqlitePreparedStatement extends SqliteStatement implements PreparedStatement {
    private final Pointer<Sqlite3.Statement> stmt;
    private final int paramCount;

    public SqlitePreparedStatement(SqliteConnection conn, Pointer<Sqlite3.Statement> stmt)
            throws SQLException {
        super(conn);

        this.stmt = requireAccess(stmt);
        this.paramCount = Sqlite3.sqlite3_bind_parameter_count(stmt);
        this.lastResult = new SqliteResultSet(this, this.stmt);
    }

    int checkParam(int index) {
        if (index < 1)
            throw new IllegalArgumentException("Parameter index must be greater than zero");
        if (index > this.paramCount)
            throw new IllegalArgumentException("Parameter index must be less than or equal to " + this.paramCount);

        return index;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
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
                Sqlite3.checkOk(rc);
                break;
        }

        return Sqlite3.sqlite3_changes(this.conn.getHandle());
    }

    @Override
    public void setNull(int i, int i2) throws SQLException {
        Sqlite3.sqlite3_bind_null(this.stmt, checkParam(i));
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        Sqlite3.sqlite3_bind_int(this.stmt, checkParam(i), b ? 1 : 0);
    }

    @Override
    public void setByte(int i, byte b) throws SQLException {
        Sqlite3.sqlite3_bind_int(this.stmt, checkParam(i), b);
    }

    @Override
    public void setShort(int i, short s) throws SQLException {
        Sqlite3.sqlite3_bind_int(this.stmt, checkParam(i), s);
    }

    @Override
    public void setInt(int i, int val) throws SQLException {
        Sqlite3.sqlite3_bind_int(this.stmt, checkParam(i), val);
    }

    @Override
    public void setLong(int i, long val) throws SQLException {
        Sqlite3.sqlite3_bind_int64(this.stmt, checkParam(i), val);
    }

    @Override
    public void setFloat(int i, float val) throws SQLException {
        Sqlite3.sqlite3_bind_double(this.stmt, checkParam(i), val);
    }

    @Override
    public void setDouble(int i, double val) throws SQLException {
        Sqlite3.sqlite3_bind_double(this.stmt, checkParam(i), val);
    }

    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        this.setString(i, bigDecimal.toString());
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        Sqlite3.checkOk(Sqlite3.sqlite3_bind_text(
                this.stmt, checkParam(i), Pointer.pointerToCString(s), -1, Sqlite3.SQLITE_TRANSIENT));
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        Pointer<Byte> ptr = Pointer.pointerToBytes(bytes);
        Sqlite3.BufferDestructorBase destructor = new Sqlite3.BufferDestructor(ptr);

        BridJ.protectFromGC(destructor);
        Sqlite3.checkOk(Sqlite3.sqlite3_bind_blob(
                this.stmt,
                i,
                ptr,
                bytes.length,
                Pointer.pointerTo(destructor)));
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTime(int i, Time time) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        Sqlite3.checkOk(Sqlite3.sqlite3_clear_bindings(this.stmt));
    }

    @Override
    public void setObject(int i, Object o, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setObject(int i, Object o) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setRef(int i, Ref ref) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setBlob(int i, Blob blob) throws SQLException {
        SqliteBlob sb = (SqliteBlob)blob;
        Sqlite3.BufferDestructorBase destructor = new Sqlite3.BufferDestructor(sb.getHandle());

        BridJ.protectFromGC(destructor);
        Sqlite3.checkOk(Sqlite3.sqlite3_bind_blob(
                this.stmt,
                checkParam(i),
                sb.getHandle(),
                (int) sb.length(),
                Pointer.pointerTo(destructor)));
    }

    @Override
    public void setClob(int i, Clob clob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setArray(int i, Array array) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.lastResult.getMetaData();
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
    public void setObject(int i, Object o, int i2, int i3) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
