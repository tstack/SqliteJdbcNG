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

import java.io.InputStream;
import java.io.Reader;
import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqliteResultSet extends SqliteCommon implements ResultSet {
    private static final String TIME_PATTERN_STRING = "(\\d{2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{3}))?)?";
    private static final Pattern TIME_PATTERN = Pattern.compile(TIME_PATTERN_STRING);

    private static final Pattern TS_PATTERN = Pattern.compile(
            "(\\d{4})-(\\d{2})-(\\d{2})[T ]" + TIME_PATTERN_STRING);

    final SqliteStatement parent;
    private final Pointer<Sqlite3.Statement> stmt;
    private final int maxRows;
    private final int columnCount;
    private final List<WeakReference<Blob>> blobList = new ArrayList<WeakReference<Blob>>();
    private final SqliteResultSetMetadata metadata;
    private boolean closed;
    private int rowNumber = 0;
    private int lastColumn;
    private final TimeoutProgressCallback timeoutCallback;
    private int lastStepResult;

    public SqliteResultSet(SqliteStatement parent, SqliteResultSetMetadata metadata, Pointer<Sqlite3.Statement> stmt, int maxRows) throws SQLException {
        this.parent = parent;
        this.metadata = metadata;
        this.metadata.setResultSet(this);
        this.stmt = stmt;
        this.columnCount = Sqlite3.sqlite3_column_count(this.stmt);
        this.maxRows = maxRows;
        this.timeoutCallback = new TimeoutProgressCallback(this.parent.conn);
    }

    private void requireOpen() throws SQLException {
        if (this.closed)
            throw new SQLNonTransientException("Result set is closed");
    }

    private void updateNotSupported() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support result set updates");
    }

    void step() throws SQLException {
        TimeoutProgressCallback cb = null;

        try {
            cb = this.timeoutCallback.setExpiration(this.parent.getQueryTimeout() * 1000);
            int rc = Sqlite3.sqlite3_step(this.stmt.getPeer());

            if (cb != null && rc == Sqlite3.ReturnCodes.SQLITE_INTERRUPT.value()) {
                throw new SQLTimeoutException("Query timeout reached");
            }

            switch (Sqlite3.ReturnCodes.valueOf(rc)) {
                case SQLITE_ROW:
                case SQLITE_DONE:
                    break;
                default:
                    Sqlite3.checkOk(rc, this.parent.getDbHandle(), true);
                    break;
            }

            this.lastStepResult = rc;
        } finally {
            closeQuietly(cb);
        }
    }

    @Override
    public synchronized boolean next() throws SQLException {
        requireOpen();
        this.clearWarnings();

        for (WeakReference<Blob> blobRef : this.blobList) {
            Blob blob = blobRef.get();

            if (blob == null) {
                continue;
            }

            blob.free();
        }
        this.blobList.clear();

        if (this.maxRows == 0 || this.rowNumber < this.maxRows) {
            if (this.rowNumber > 0) {
                step();
            }
            this.rowNumber += 1;
            switch (Sqlite3.ReturnCodes.valueOf(this.lastStepResult)) {
                case SQLITE_ROW:
                    return true;
                case SQLITE_DONE:
                    return false;
                default:
                    return false;
            }
        }
        else {
            return false;
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        if (!this.closed) {
            this.closed = true;

            if (this.rowNumber > 0 && this.stmt.get() != null) {
                Sqlite3.sqlite3_reset(this.stmt);
                this.rowNumber = 0;
            }
            if (!(this.parent instanceof SqlitePreparedStatement)) {
                Sqlite3.sqlite3_finalize(this.stmt);
            }
            this.lastColumn = -1;

            this.parent.resultSetClosed();
        }
    }

    int checkColumnIndex(int i) throws SQLException {
        if (i < 1)
            throw new SQLNonTransientException("Column index must be greater than zero");
        if (i > this.columnCount)
            throw new SQLNonTransientException("Column index must be less than or equal to " + this.columnCount);

        return i - 1;
    }

    private int checkColumn(int i) throws SQLException {
        requireOpen();

        if (this.rowNumber == 0)
            throw new SQLNonTransientException("The next() method must be called before getting any data.");
        this.checkColumnIndex(i);

        this.lastColumn = i;

        return i - 1;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return (Sqlite3.sqlite3_column_type(this.stmt.getPeer(), checkColumn(this.lastColumn)) ==
                Sqlite3.DataType.SQLITE_NULL.value());
    }

    @Override
    public String getString(int i) throws SQLException {
        long ptr = Sqlite3.sqlite3_column_text(stmt.getPeer(), checkColumn(i));
        Pointer<String> str = Pointer.pointerToAddress(ptr, String.class, null);

        if (str != null) {
            return str.getCString();
        }

        return null;
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        return this.getInt(i) != 0;
    }

    @Override
    public byte getByte(int i) throws SQLException {
        return (byte)this.getInt(i);
    }

    @Override
    public short getShort(int i) throws SQLException {
        return (short)this.getInt(i);
    }

    @Override
    public int getInt(int i) throws SQLException {
        return Sqlite3.sqlite3_column_int(this.stmt.getPeer(), checkColumn(i));
    }

    @Override
    public long getLong(int i) throws SQLException {
        return Sqlite3.sqlite3_column_int64(this.stmt.getPeer(), checkColumn(i));
    }

    @Override
    public float getFloat(int i) throws SQLException {
        return (float)Sqlite3.sqlite3_column_double(this.stmt.getPeer(), checkColumn(i));
    }

    @Override
    public double getDouble(int i) throws SQLException {
        return Sqlite3.sqlite3_column_double(this.stmt.getPeer(), checkColumn(i));
    }

    @Override
    public BigDecimal getBigDecimal(int i, int i2) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(int i) throws SQLException {
        int zcol = checkColumn(i);
        long ptr = Sqlite3.sqlite3_column_blob(this.stmt.getPeer(), zcol);
        int blobLen = Sqlite3.sqlite3_column_bytes(this.stmt.getPeer(), zcol);
        Pointer<Byte> blob = Pointer.pointerToAddress(ptr, Byte.class);

        return blob != null ? blob.getBytes(blobLen) : null;
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return this.getDate(i, null);
    }

    @Override
    public Time getTime(int i) throws SQLException {
        Calendar cal = DEFAULT_CALENDAR.get();

        /*
         * http://www.sqlite.org/lang_datefunc.html
         */
        cal.clear();
        cal.set(Calendar.YEAR, 2000);
        cal.set(Calendar.MONTH, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        return this.getTime(i, cal);
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return this.getTimestamp(i, null);
    }

    @Override
    public InputStream getAsciiStream(int i) throws SQLException {
        return this.getBinaryStream(i);
    }

    @Override
    public InputStream getUnicodeStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getBinaryStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getString(String s) throws SQLException {
        return this.getString(this.findColumn(s));
    }

    @Override
    public boolean getBoolean(String s) throws SQLException {
        return this.getBoolean(this.findColumn(s));
    }

    @Override
    public byte getByte(String s) throws SQLException {
        return this.getByte(this.findColumn(s));
    }

    @Override
    public short getShort(String s) throws SQLException {
        return this.getShort(this.findColumn(s));
    }

    @Override
    public int getInt(String s) throws SQLException {
        return this.getInt(this.findColumn(s));
    }

    @Override
    public long getLong(String s) throws SQLException {
        return this.getLong(this.findColumn(s));
    }

    @Override
    public float getFloat(String s) throws SQLException {
        return this.getFloat(this.findColumn(s));
    }

    @Override
    public double getDouble(String s) throws SQLException {
        return this.getDouble(this.findColumn(s));
    }

    @Override
    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
        return this.getBigDecimal(this.findColumn(s), i);
    }

    @Override
    public byte[] getBytes(String s) throws SQLException {
        return this.getBytes(this.findColumn(s));
    }

    @Override
    public Date getDate(String s) throws SQLException {
        return this.getDate(this.findColumn(s));
    }

    @Override
    public Time getTime(String s) throws SQLException {
        return this.getTime(this.findColumn(s));
    }

    @Override
    public Timestamp getTimestamp(String s) throws SQLException {
        return this.getTimestamp(this.findColumn(s));
    }

    @Override
    public InputStream getAsciiStream(String s) throws SQLException {
        return this.getAsciiStream(this.findColumn(s));
    }

    @Override
    public InputStream getUnicodeStream(String s) throws SQLException {
        return this.getUnicodeStream(this.findColumn(s));
    }

    @Override
    public InputStream getBinaryStream(String s) throws SQLException {
        return this.getBinaryStream(this.findColumn(s));
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support named cursors");
    }

    @Override
    public synchronized ResultSetMetaData getMetaData() throws SQLException {
        requireOpen();

        return this.metadata;
    }

    @Override
    public Object getObject(int i) throws SQLException {
        Sqlite3.DataType dt;

        dt = Sqlite3.DataType.valueOf(Sqlite3.sqlite3_column_type(this.stmt.getPeer(), this.checkColumn(i)));
        switch (dt) {
            case SQLITE_NULL:
                return null;
            case SQLITE_FLOAT:
                return this.getDouble(i);
            case SQLITE_INTEGER: {
                long bigint = Sqlite3.sqlite3_column_int64(this.stmt.getPeer(), this.checkColumn(i));

                if (Integer.MIN_VALUE <= bigint && bigint <= Integer.MAX_VALUE) {
                    return (int)bigint;
                }

                return bigint;
            }
            case SQLITE_TEXT:
                return this.getString(i);
            case SQLITE_BLOB:
                return this.getBlob(i);
            default:
                throw new RuntimeException("Unhandled sqlite3 type" + dt);
        }
    }

    @Override
    public Object getObject(String s) throws SQLException {
        return this.getObject(this.findColumn(s));
    }

    @Override
    public int findColumn(String s) throws SQLException {
        return this.metadata.findColumn(s);
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        return this.getCharacterStream(this.findColumn(s));
    }

    /**
     * Implementation note: SQLite does not preserve the precision/scale of
     * NUMERIC or DECIMAL values, which Java's BigDecimal does.  So, you
     * cannot expect to be able to insert a BigDecimal into a SQLite DB and
     * get the exact same value back out.  If you are looking to test for
     * equality, you should use the BigDecimal.compareTo() method instead
     * of equals().
     *
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        String str = this.getString(i);
        BigDecimal retval = null;

        if (str != null) {
            try {
                retval = new BigDecimal(str);
            }
            catch (NumberFormatException e) {
                throw new SQLDataException("Cannot convert string to BigDecimal: " + str, e);
            }
        }

        return retval;
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws SQLException {
        return this.getBigDecimal(this.findColumn(s));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void beforeFirst() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void afterLast() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean first() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean last() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getRow() throws SQLException {
        return this.rowNumber;
    }

    @Override
    public boolean absolute(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean relative(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean previous() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("SQLite only supports FETCH_FORWARD result sets");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNull(int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateByte(int i, byte b) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateShort(int i, short i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateInt(int i, int i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateLong(int i, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateFloat(int i, float v) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateDouble(int i, double v) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateString(int i, String s) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateDate(int i, Date date) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateTime(int i, Time time) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateObject(int i, Object o, int i2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNull(String s) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBoolean(String s, boolean b) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateByte(String s, byte b) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateShort(String s, short i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateInt(String s, int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateLong(String s, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateFloat(String s, float v) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateDouble(String s, double v) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateString(String s, String s2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBytes(String s, byte[] bytes) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateDate(String s, Date date) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateTime(String s, Time time) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void insertRow() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateRow() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void deleteRow() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void refreshRow() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        updateNotSupported();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.parent;
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> stringClassMap) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support REF values");
    }

    @Override
    public synchronized Blob getBlob(int i) throws SQLException {
        long peer = Sqlite3.sqlite3_column_blob(this.stmt.getPeer(), checkColumn(i));
        int len = Sqlite3.sqlite3_column_bytes(this.stmt.getPeer(), checkColumn(i));
        Pointer<Byte> ptr = Pointer.pointerToAddress(peer, Byte.class);

        if (ptr == null) {
            return null;
        }

        SqliteBlob retval = new SqliteBlob(ptr.validBytes(len));

        this.blobList.add(new WeakReference<Blob>(retval));

        return retval;
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array getArray(int i) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support arrays");
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support REF values");
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return this.getBlob(this.findColumn(s));
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array getArray(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support arrays");
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        String dateString = this.getString(i);
        SimpleDateFormat dateFormat = DATE_FORMATTER.get();

        if (dateString == null)
            return null;

        if (calendar == null)
            calendar = DEFAULT_CALENDAR.get();

        try {
            dateFormat.setCalendar(calendar);
            dateFormat.parse(dateString);
            return new Date(calendar.getTime().getTime());
        } catch (ParseException e) {
            throw new SQLDataException("Invalid date", e);
        }
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return this.getDate(this.findColumn(s), calendar);
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        String timeString = this.getString(i);

        if (timeString == null)
            return null;

        if (calendar == null) {
            calendar = DEFAULT_CALENDAR.get();
            calendar.clear();
        }

        Matcher m = TIME_PATTERN.matcher(timeString);
        String val;

        if (!m.matches()) {
            throw new SQLDataException("Invalid time -- " + timeString);
        }

        calendar.set(Calendar.HOUR_OF_DAY, Integer.valueOf(m.group(1)));
        calendar.set(Calendar.MINUTE, Integer.valueOf(m.group(2)));
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        val = m.group(3);
        if (val != null) {
            calendar.set(Calendar.SECOND, Integer.valueOf(m.group(3)));
            val = m.group(4);
            if (val != null) {
                calendar.set(Calendar.MILLISECOND, Integer.valueOf(m.group(4)));
            }
        }

        return new Time(calendar.getTime().getTime());
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return this.getTime(this.findColumn(s), calendar);
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        String optval, value = this.getString(i);

        if (value == null)
            return null;

        Calendar cal = DEFAULT_CALENDAR.get();
        Matcher m = TS_PATTERN.matcher(value);

        if (!m.matches()) {
            throw new SQLDataException("Bad timestamp value -- " + value);
        }

        cal.clear();
        cal.set(Calendar.YEAR, Integer.valueOf(m.group(1)));
        cal.set(Calendar.MONTH, Integer.valueOf(m.group(2)) - 1);
        cal.set(Calendar.DAY_OF_MONTH, Integer.valueOf(m.group(3)));
        cal.set(Calendar.HOUR, Integer.valueOf(m.group(4)));
        cal.set(Calendar.MINUTE, Integer.valueOf(m.group(5)));
        optval = m.group(6);
        if (optval != null) {
            cal.set(Calendar.SECOND, Integer.valueOf(optval));
            optval = m.group(7);
            if (optval != null) {
                cal.set(Calendar.MILLISECOND, Integer.valueOf(optval));
            }
        }

        return new Timestamp(cal.getTimeInMillis());
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return this.getTimestamp(this.findColumn(s), calendar);
    }

    @Override
    public URL getURL(int i) throws SQLException {
        try {
            return new URL(this.getString(i));
        }
        catch (MalformedURLException e) {
            throw new SQLDataException("Invalid URL", "", e);
        }
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return this.getURL(this.findColumn(s));
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateRef(String s, Ref ref) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(String s, Blob blob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(String s, Clob clob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateArray(int i, Array array) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateArray(String s, Array array) throws SQLException {
        updateNotSupported();
    }

    @Override
    public RowId getRowId(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RowId getRowId(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateRowId(int i, RowId rowId) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateRowId(String s, RowId rowId) throws SQLException {
        updateNotSupported();
    }

    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void updateNString(int i, String s) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNString(String s, String s2) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(int i, NClob nClob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(String s, NClob nClob) throws SQLException {
        updateNotSupported();
    }

    @Override
    public NClob getNClob(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NClob getNClob(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLXML getSQLXML(int i) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLite does not support SQLXML");
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return this.getSQLXML(this.findColumn(s));
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
        updateNotSupported();
    }

    @Override
    public String getNString(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getNString(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reader getNCharacterStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reader getNCharacterStream(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(int i, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(String s, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(int i, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(String s, Reader reader, long l) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(int i, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateCharacterStream(String s, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(int i, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateBlob(String s, InputStream inputStream) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(int i, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateClob(String s, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(int i, Reader reader) throws SQLException {
        updateNotSupported();
    }

    @Override
    public void updateNClob(String s, Reader reader) throws SQLException {
        updateNotSupported();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return this.getObject(this.findColumn(columnLabel), type);
    }
}
