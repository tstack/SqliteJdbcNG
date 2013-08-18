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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class SqliteResultSetMetadata implements ResultSetMetaData {
    private final SqliteResultSet rs;
    private final boolean readOnly;
    private final int columnCount;
    private final String[] columnNames;
    private final String[] columnDeclType;
    private final String[] databaseNames;
    private final String[] tableNames;
    private final String[] originNames;

    public SqliteResultSetMetadata(SqliteResultSet rs) {
        this.rs = rs;
        this.readOnly = Sqlite3.sqlite3_stmt_readonly(rs.getHandle()) != 0;
        this.columnCount = Sqlite3.sqlite3_column_count(rs.getHandle());
        this.columnNames = new String[this.columnCount];
        this.columnDeclType = new String[this.columnCount];
        this.databaseNames = new String[this.columnCount];
        this.tableNames = new String[this.columnCount];
        this.originNames = new String[this.columnCount];

        for (int lpc = 0; lpc < this.columnCount; lpc++) {
            Pointer<Byte> ptr = Sqlite3.sqlite3_column_name(rs.getHandle(), lpc);

            if (ptr == null)
                throw new OutOfMemoryError();
            this.columnNames[lpc] = ptr.getCString();

            ptr = Sqlite3.sqlite3_column_decltype(rs.getHandle(), lpc);
            if (ptr == null) {
                Sqlite3.DataType dt;
                int exprType;

                exprType = Sqlite3.sqlite3_column_type(rs.getHandle(), lpc);
                dt = Sqlite3.DataType.valueOf(exprType);
                this.columnDeclType[lpc] = dt.getSqlType();
            }
            else {
                this.columnDeclType[lpc] = ptr.getCString();
            }

            try {
                ptr = Sqlite3.sqlite3_column_database_name(rs.getHandle(), lpc);
                this.databaseNames[lpc] = ptr != null ? ptr.getCString() : "";
                ptr = Sqlite3.sqlite3_column_table_name(rs.getHandle(), lpc);
                this.tableNames[lpc] = ptr != null ? ptr.getCString() : "";
                ptr = Sqlite3.sqlite3_column_origin_name(rs.getHandle(), lpc);
                this.originNames[lpc] = ptr != null ? ptr.getCString() : "";
            }
            catch (UnsatisfiedLinkError e) {
                this.databaseNames[lpc] = "";
                this.tableNames[lpc] = "";
                this.originNames[lpc] = "";
            }
        }
    }

    int findColumn(String label) throws SQLException {
        for (int lpc = 0; lpc < this.columnNames.length; lpc++) {
            if (this.columnNames[lpc].equals(label))
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
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isCaseSensitive(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
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
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getColumnDisplaySize(int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return this.columnNames[this.rs.checkColumnIndex(i)];
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        return this.originNames[this.rs.checkColumnIndex(i)];
    }

    @Override
    public String getSchemaName(int i) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getScale(int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getTableName(int i) throws SQLException {
        return this.tableNames[this.rs.checkColumnIndex(i)];
    }

    @Override
    public String getCatalogName(int i) throws SQLException {
        return this.databaseNames[this.rs.checkColumnIndex(i)];
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        return this.columnDeclType[this.rs.checkColumnIndex(i)];
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
