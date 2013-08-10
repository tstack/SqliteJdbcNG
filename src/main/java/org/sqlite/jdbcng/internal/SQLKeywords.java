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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SQLKeywords {
    private static final String[] readResource(String name) {
        InputStream is = SQLKeywords.class.getResourceAsStream(name);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            List<String> keywords = new ArrayList<String>();
            String line;

            while ((line = reader.readLine()) != null) {
                keywords.add(line);
            }

            return keywords.toArray(new String[keywords.size()]);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read resource file");
        }
    }

    private final String[] sqlKeywords;
    private final String[] sqliteKeywords;

    public SQLKeywords() {
        /*
         * http://developer.mimer.com/validator/sql-reserved-words.tml
         * sqlite-src/tool/mkkeywordhash.c
         */
        this.sqlKeywords = readResource("/sql-keywords.txt");
        this.sqliteKeywords = readResource("/sqlite-keywords.txt");
    }

    public String[] getSqlKeywords() {
        return this.sqlKeywords;
    }

    public String[] getSqliteKeywords() {
        return this.sqliteKeywords;
    }
}
