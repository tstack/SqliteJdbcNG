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

package org.sqlitejdbcng.internal;

import org.sqlitejdbcng.SqliteCommon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SQLKeywords {
    static final String[] readResource(String name) throws IOException {
        InputStream is = SQLKeywords.class.getResourceAsStream(name);
        BufferedReader reader = null;

        if (is == null)
            throw new RuntimeException("Bad resource name -- " + name);
        try {
            reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            List<String> keywords = new ArrayList<String>();
            String line;

            while ((line = reader.readLine()) != null) {
                keywords.add(line);
            }

            return keywords.toArray(new String[keywords.size()]);
        } finally {
            SqliteCommon.closeQuietly(reader);
            SqliteCommon.closeQuietly(is);
        }
    }

    private final String[] sqlKeywords;
    private final String[] sqliteKeywords;

    public SQLKeywords() {
        /*
         * http://developer.mimer.com/validator/sql-reserved-words.tml
         * sqlite-src/tool/mkkeywordhash.c
         */
        try {
            this.sqlKeywords = readResource("/sql-keywords.txt");
            this.sqliteKeywords = readResource("/sqlite-keywords.txt");
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to read resources?", e);
        }
    }

    public String[] getSqlKeywords() {
        return this.sqlKeywords;
    }

    public String[] getSqliteKeywords() {
        return this.sqliteKeywords;
    }
}
