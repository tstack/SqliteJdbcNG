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

import java.util.Locale;
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

        this.path = userPath;
    }

    public String getPath() {
        return this.path;
    }

    public static boolean isSqliteUrl(String url) {
        return url.toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }
}
