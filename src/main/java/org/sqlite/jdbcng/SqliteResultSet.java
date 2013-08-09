package org.sqlite.jdbcng;

import org.bridj.Pointer;
import org.sqlite.jdbcng.bridj.Sqlite3;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class SqliteResultSet implements ResultSet {
    private final SqliteStatement parent;
    private final Pointer<Sqlite3.Statement> stmt;
    private final int columnCount;
    private SqliteResultSetMetadata metadata;
    private boolean closed;
    private int lastColumn;

    public SqliteResultSet(SqliteStatement parent, Pointer<Sqlite3.Statement> stmt) {
        this.parent = parent;
        this.stmt = stmt;
        this.columnCount = Sqlite3.sqlite3_column_count(this.stmt);
    }

    public Pointer<Sqlite3.Statement> getHandle() {
        return this.stmt;
    }

    @Override
    public boolean next() throws SQLException {
        int rc;

        rc = Sqlite3.sqlite3_step(this.stmt);
        switch (Sqlite3.ReturnCodes.valueOf(rc)) {
            case SQLITE_ROW:
                return true;
            case SQLITE_DONE:
                return false;
            default:
                Sqlite3.checkOk(rc);
                return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (!this.closed) {
            Sqlite3.checkOk(Sqlite3.sqlite3_reset(this.stmt));
            this.lastColumn = -1;
            this.closed = true;
        }
    }

    private void requireOpen() throws SQLException {
        if (this.closed)
            throw new SQLNonTransientException("Result set is closed");
    }

    int checkColumn(int i) throws SQLException {
        requireOpen();

        if (i < 1)
            throw new IllegalArgumentException("Column index must be greater than zero");
        if (i > this.columnCount)
            throw new IllegalArgumentException("Column index must be less than or equal to " + this.columnCount);

        this.lastColumn = i;

        return i - 1;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return (Sqlite3.sqlite3_column_type(this.stmt, checkColumn(this.lastColumn)) ==
                Sqlite3.DataType.SQLITE_NULL.value());
    }

    @Override
    public String getString(int i) throws SQLException {
        Pointer<Byte> str = Sqlite3.sqlite3_column_text(stmt, checkColumn(i));

        return str.getCString();
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
        return Sqlite3.sqlite3_column_int(this.stmt, checkColumn(i));
    }

    @Override
    public long getLong(int i) throws SQLException {
        return Sqlite3.sqlite3_column_int64(this.stmt, checkColumn(i));
    }

    @Override
    public float getFloat(int i) throws SQLException {
        return (float)Sqlite3.sqlite3_column_double(this.stmt, checkColumn(i));
    }

    @Override
    public double getDouble(int i) throws SQLException {
        return Sqlite3.sqlite3_column_double(this.stmt, checkColumn(i));
    }

    @Override
    public BigDecimal getBigDecimal(int i, int i2) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes(int i) throws SQLException {
        int zcol = checkColumn(i);
        Pointer<Byte> blob = Sqlite3.sqlite3_column_blob(this.stmt, zcol);
        int blobLen = Sqlite3.sqlite3_column_bytes(this.stmt, zcol);

        return blob.getBytes(blobLen);
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Time getTime(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getAsciiStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getBoolean(String s) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte getByte(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public short getShort(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getInt(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getLong(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getFloat(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public double getDouble(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] getBytes(String s) throws SQLException {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Date getDate(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Time getTime(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Timestamp getTimestamp(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getAsciiStream(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getUnicodeStream(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getBinaryStream(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
    public String getCursorName() throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public synchronized ResultSetMetaData getMetaData() throws SQLException {
        if (this.metadata == null)
            this.metadata = new SqliteResultSetMetadata(this);

        return this.metadata;
    }

    @Override
    public Object getObject(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getObject(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int findColumn(String s) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
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
    public int getType() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateByte(int i, byte b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateShort(int i, short i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateInt(int i, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateLong(int i, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateFloat(int i, float v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateDouble(int i, double v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateString(int i, String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateDate(int i, Date date) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateTime(int i, Time time) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateObject(int i, Object o, int i2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNull(String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBoolean(String s, boolean b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateByte(String s, byte b) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateShort(String s, short i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateInt(String s, int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateLong(String s, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateFloat(String s, float v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateDouble(String s, double v) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateString(String s, String s2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBytes(String s, byte[] bytes) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateDate(String s, Date date) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateTime(String s, Time time) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void insertRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deleteRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void refreshRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array getArray(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public URL getURL(int i) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateRef(String s, Ref ref) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(String s, Blob blob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(String s, Clob clob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateArray(int i, Array array) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateArray(String s, Array array) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateRowId(String s, RowId rowId) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void updateNString(int i, String s) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNString(String s, String s2) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(int i, NClob nClob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(String s, NClob nClob) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(String s, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(int i, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(String s, Reader reader, long l) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateCharacterStream(String s, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(int i, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateBlob(String s, InputStream inputStream) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateClob(String s, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(int i, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateNClob(String s, Reader reader) throws SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
