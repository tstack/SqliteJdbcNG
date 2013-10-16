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

import org.sqlitejdbcng.bridj.Sqlite3;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SqliteDriver implements Driver {
    private static final Logger LOGGER = Logger.getLogger(SqliteDriver.class.getPackage().getName());

    static {
        try {
            /*
             * Apparently the DriverManager service loader does not register
             * the instance of our class that instantiates.  So, we still
             * need to do the registration statically.
             */
            DriverManager.registerDriver(new SqliteDriver());
        }
        catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Could not register SqliteDriver", e);
        }
    }

    static final int[] VERSION = { 0, 5 };

    public SqliteDriver() {
        LOGGER.log(Level.INFO,
                "SQLite library version {0} -- {1}",
                new Object[] {
                        Sqlite3.sqlite3_libversion().getCString(),
                        Sqlite3.sqlite3_sourceid().getCString()
                });
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        Connection retval = null;

        if (SqliteUrl.isSqliteUrl(url)) {
            retval = new SqliteConnection(url, properties);
        }

        return retval;
    }

    @Override
    public boolean acceptsURL(String s) throws SQLException {
        return SqliteUrl.isSqliteUrl(s);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return VERSION[0];
    }

    @Override
    public int getMinorVersion() {
        return VERSION[1];
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return LOGGER;
    }
}
