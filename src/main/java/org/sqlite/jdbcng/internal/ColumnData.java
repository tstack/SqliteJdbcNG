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

package org.sqlite.jdbcng.internal;

import org.bridj.Pointer;
import org.sqlite.jdbcng.bridj.Sqlite3;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnData {
    private static final Pattern TYPE_PATTERN = Pattern.compile(
            "([A-Z_0-9 ]+)\\s*" +
                    "(?:\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+))?\\s*\\))?");

    private static final Map<String, Integer> TYPE_MAP = new HashMap<>();

    static {
        /* XXX this isn't the right way to do this... */
        TYPE_MAP.put("INT", Types.INTEGER);
        TYPE_MAP.put("MEDIUMINT", Types.INTEGER);
        TYPE_MAP.put("INT2", Types.INTEGER);
        TYPE_MAP.put("INT4", Types.INTEGER);
        TYPE_MAP.put("INT8", Types.INTEGER);
        TYPE_MAP.put("UNSIGNED BIG INT", Types.BIGINT);

        TYPE_MAP.put("NATIVE CHARACTER", Types.NCHAR);
        TYPE_MAP.put("VARYING CHARACTER", Types.NCHAR);
        TYPE_MAP.put("TEXT", Types.VARCHAR);
        TYPE_MAP.put("CHARACTER", Types.CHAR);

        TYPE_MAP.put("DOUBLE PRECISION", Types.REAL);

        TYPE_MAP.put("DATETIME", Types.TIMESTAMP);
    }

    public final String dbName;
    public final String tableName;
    public final int index;
    public final String name;
    public final String fullType;
    public final String type;
    public final int precision;
    public final int scale;
    public final int sqlType;
    public final int notNull;
    public final String defaultValue;
    public final int primaryKey;
    public final boolean autoInc;

    public ColumnData(Pointer<Sqlite3.Sqlite3Db> db,
                      String dbName,
                      String tableName,
                      String name,
                      int index,
                      String defaultValue,
                      String fullType,
                      int notNull,
                      int primaryKey,
                      boolean autoInc) {
        int parenIndex, sqlType = Types.VARCHAR;
        int precision, scale;
        Matcher m;

        this.dbName = dbName;
        this.tableName = tableName;
        this.index = index;
        this.name = name;
        this.fullType = fullType.toUpperCase();

        precision = Sqlite3.sqlite3_limit(db, Sqlite3.Limit.SQLITE_LIMIT_LENGTH.value(), -1);
        scale = 0;
        m = TYPE_PATTERN.matcher(this.fullType);
        if (m.matches()) {
            this.type = m.group(1).trim();
            if (m.group(2) != null) {
                precision = Integer.valueOf(m.group(2));
                if (m.group(3) != null)
                    scale = Integer.valueOf(m.group(3));
            }
        }
        else {
            this.type = "VARCHAR";
        }
        this.precision = precision;
        this.scale = scale;

        try {
            Field typeField = Types.class.getField(this.type);

            sqlType = typeField.getInt(Types.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Integer mappedType = TYPE_MAP.get(this.type);

            if (mappedType == null)
                sqlType = Types.VARCHAR;
            else
                sqlType = mappedType;
        }
        this.sqlType = sqlType;
        this.notNull = notNull;
        this.defaultValue = defaultValue;
        this.primaryKey = primaryKey;
        this.autoInc = autoInc;
    }

    public ColumnData(Pointer<Sqlite3.Sqlite3Db> db, String dbName, String tableName, ResultSet rs) throws SQLException {
        this(db, dbName,
                tableName,
                rs.getString("name"),
                rs.getInt("cid") + 1,
                rs.getString("dflt_value"),
                rs.getString("type"),
                rs.getBoolean("notnull") ? ResultSetMetaData.columnNoNulls :
                ResultSetMetaData.columnNullable,
                rs.getInt("pk"),
                false);
    }
}
