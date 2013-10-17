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
import org.sqlitejdbcng.internal.ColumnData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class SqliteResultSetMetadata implements ResultSetMetaData {
    private final Pointer<Sqlite3.Statement> stmt;
    private final int columnCount;
    private final ColumnData[] columnList;
    private final String[] columnLabels;
    private SqliteResultSet rs;

    public SqliteResultSetMetadata(Pointer<Sqlite3.Statement> stmt) {
        this.stmt = stmt;
        this.columnCount = Sqlite3.sqlite3_column_count(stmt);
        this.columnLabels = new String[this.columnCount];
        this.columnList = new ColumnData[this.columnCount];

        for (int lpc = 0; lpc < this.columnCount; lpc++) {
            Pointer<Byte> ptr = Sqlite3.sqlite3_column_name(stmt, lpc);

            if (ptr == null)
                throw new OutOfMemoryError();
            this.columnLabels[lpc] = ptr.getCString();
        }

    }

    void setResultSet(SqliteResultSet rs) {
        this.rs = rs;
    }

    private int checkColumnIndex(int inIndex) throws SQLException {
        int retval = this.rs.checkColumnIndex(inIndex);

        if (this.columnList[retval] != null) {
            return retval;
        }

        int notNull = columnNullableUnknown;
        Pointer<Byte> ptr;
        int primaryKey = 0;
        boolean autoInc = false;
        String type;

        ptr = Sqlite3.sqlite3_column_decltype(stmt, retval);
        if (ptr == null) {
            Sqlite3.DataType dt;
            int exprType;

            exprType = Sqlite3.sqlite3_column_type(stmt.getPeer(), retval);
            dt = Sqlite3.DataType.valueOf(exprType);
            type = dt.getSqlType();
        }
        else {
            type = ptr.getCString();
        }

        String dbName = "", tableName = "", columnName = "";

        try {
            if ((ptr = Sqlite3.sqlite3_column_database_name(stmt, retval)) != null)
                dbName = ptr.getCString();
            if ((ptr = Sqlite3.sqlite3_column_table_name(stmt, retval)) != null)
                tableName = ptr.getCString();
                if ((ptr = Sqlite3.sqlite3_column_origin_name(stmt, retval)) != null)
                    columnName = ptr.getCString();
                if (!dbName.isEmpty()) {
                    Pointer<Pointer<Byte>> dataType = Pointer.allocatePointer(Byte.class);
                    Pointer<Pointer<Byte>> collSeq = Pointer.allocatePointer(Byte.class);
                    Pointer<Integer> notNullInt = Pointer.allocateInt();
                    Pointer<Integer> primaryKeyInt = Pointer.allocateInt();
                    Pointer<Integer> autoIncInt = Pointer.allocateInt();

                    Sqlite3.sqlite3_table_column_metadata(this.rs.parent.getDbHandle(),
                            Pointer.pointerToCString(dbName),
                            Pointer.pointerToCString(tableName),
                            Pointer.pointerToCString(columnName),
                            dataType,
                            collSeq,
                            notNullInt,
                            primaryKeyInt,
                            autoIncInt);

                    notNull = notNullInt.getInt() != 0 ? columnNoNulls : columnNullable;

                }
        }
        catch (UnsatisfiedLinkError e) {
        }

        this.columnList[retval] = new ColumnData(
                this.rs.parent.getDbHandle(),
                dbName,
                tableName,
                columnName,
                -1,
                "",
                type,
                notNull,
                primaryKey,
                autoInc);

        return retval;
    }

    int findColumn(String label) throws SQLException {
        for (int lpc = 0; lpc < this.columnLabels.length; lpc++) {
            if (this.columnLabels[lpc].equals(label))
                return lpc + 1;
        }

        throw new SQLNonTransientException("Result set does not contain label -- " + label);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.columnCount;
    }

    @Override
    public boolean isAutoIncrement(int i) throws SQLException {
        return this.columnList[this.rs.checkColumnIndex(i)].autoInc;
    }

    @Override
    public boolean isCaseSensitive(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int i) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int i) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int i) throws SQLException {
        return this.columnList[this.rs.checkColumnIndex(i)].notNull;
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        return !this.getColumnTypeName(i).startsWith("UNSIGNED");
    }

    @Override
    public int getColumnDisplaySize(int i) throws SQLException {
        return this.getPrecision(i) + 1;
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return this.columnLabels[this.rs.checkColumnIndex(i)];
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        return this.columnList[this.rs.checkColumnIndex(i)].name;
    }

    @Override
    public String getSchemaName(int i) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].precision;
    }

    @Override
    public int getScale(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].scale;
    }

    @Override
    public String getTableName(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].tableName;
    }

    @Override
    public String getCatalogName(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].dbName;
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].sqlType;
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        return this.columnList[this.checkColumnIndex(i)].type;
    }

    @Override
    public boolean isReadOnly(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int i) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int i) throws SQLException {
        return true;
    }

    @Override
    public String getColumnClassName(int i) throws SQLException {
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
