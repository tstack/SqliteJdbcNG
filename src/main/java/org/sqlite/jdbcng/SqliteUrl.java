package org.sqlite.jdbcng;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqliteUrl {
    public static final String PREFIX = "jdbc:sqlite:";

    private static final Pattern SQLITE_URL_PATTERN = Pattern.compile(PREFIX + "(.*)", Pattern.CASE_INSENSITIVE);

    private final String path;

    public SqliteUrl(String url) {
        Matcher matcher = SQLITE_URL_PATTERN.matcher(url);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a JDBC sqlite URL: " + url);
        }

        String userPath = matcher.group(1);

        if (userPath.isEmpty())
            userPath = ":memory";

        this.path = userPath;

        System.out.println("path " + this.path);
    }

    public String getPath() {
        return this.path;
    }

    public static boolean isSqliteUrl(String url) {
        return url.toLowerCase().startsWith(PREFIX);
    }
}
