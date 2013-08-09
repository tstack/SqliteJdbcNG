package org.sqlite.jdbcng;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class SqliteDriver implements Driver {
    public static final int[] VERSION = { 0, 5 };

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
        return new DriverPropertyInfo[0];  //To change body of implemented methods use File | Settings | File Templates.
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
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
