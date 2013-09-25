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

import org.sqlitejdbcng.bridj.Sqlite3;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FunctionEscapeHandler implements EscapeHandler {
    private static final Pattern FUNC_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)\\s*(\\(.*\\))?$");

    private static final Map<String, String> SIMPLE_MAPPINGS = new HashMap<>();

    static {
        SIMPLE_MAPPINGS.put("CHAR_LENGTH", "LENGTH");
        SIMPLE_MAPPINGS.put("OCTET_LENGTH", "LENGTH");
        SIMPLE_MAPPINGS.put("LCASE", "LOWER");
        SIMPLE_MAPPINGS.put("UCASE", "UPPER");
        SIMPLE_MAPPINGS.put("SUBSTRING", "SUBSTR");
    }

    @Override
    public String handle(String keyword, String arg) throws SQLException {
        Matcher m = FUNC_PATTERN.matcher(arg);

        if (m.matches()) {
            String name = m.group(1).toUpperCase();
            String mappedName = SIMPLE_MAPPINGS.get(name);

            if (mappedName != null) {
                return mappedName + m.group(2);
            }

            if ("USER".equals(name)) {
                return "''";
            }
            if ("CONCAT".equals(name)) {
                String funcArgs = m.group(2);
                String[] args = EscapeParser.split(funcArgs.substring(1, funcArgs.length() - 1));

                return Sqlite3.join(args, " || ");
            }
        }

        return arg;
    }
}
