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

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EscapeParser {
    enum ParserState {
        STATE_SQL(""),
        STATE_SINGLE_STRING("Unterminated single quote"),
        STATE_DOUBLE_STRING("Unterminated double quote"),
        STATE_BRACKET("Unterminated bracket"),
        STATE_ESCAPE_KEYWORD_START("Incomplete escape sequence"),
        STATE_ESCAPE_KEYWORD("Incomplete escape sequence");

        private final String msg;

        ParserState(String msg) {
            this.msg = msg;
        }

        public String getMessage() {
            return this.msg;
        }
    };

    private EscapeParser() {

    }

    private static int split(int depth, String sql, int start, List<String> accum) {
        ParserState state = ParserState.STATE_SQL;

        for (int lpc = start; lpc < sql.length(); lpc++) {
            char ch = sql.charAt(lpc);

            switch (state) {
                case STATE_SQL:
                    switch (ch) {
                        case ',':
                            if (depth == 0) {
                                accum.add(sql.substring(start, lpc).trim());
                                start = lpc + 1;
                            }
                            break;
                        case '(':
                            lpc = split(depth + 1, sql, lpc + 1, accum);
                            break;
                        case ')':
                            return lpc;
                        case '\'':
                            state = ParserState.STATE_SINGLE_STRING;
                            break;
                        case '"':
                            state = ParserState.STATE_DOUBLE_STRING;
                            break;
                        default:
                            break;
                    }
                    break;
                case STATE_SINGLE_STRING:
                    if (ch == '\'')
                        state = ParserState.STATE_SQL;
                    break;
                case STATE_DOUBLE_STRING:
                    if (ch == '"')
                        state = ParserState.STATE_SQL;
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        if (depth == 0) {
            accum.add(sql.substring(start).trim());
        }

        return sql.length();
    }

    public static String[] split(String sql) {
        List<String> accum = new ArrayList<>();

        split(0, sql, 0, accum);
        return accum.toArray(new String[accum.size()]);
    }

    private static int transform(int depth, String escapedSql, int start, StringBuilder dest, Map<String, EscapeHandler> handlerMap) throws SQLException {
        ParserState state = ParserState.STATE_SQL;
        int keywordStart = -1, retval = -1;

        for (int lpc = start; lpc < escapedSql.length(); lpc++) {
            char ch = escapedSql.charAt(lpc);

            switch (state) {
                case STATE_SQL:
                    switch (ch) {
                        case '\'':
                            state = ParserState.STATE_SINGLE_STRING;
                            break;
                        case '"':
                            state = ParserState.STATE_DOUBLE_STRING;
                            break;
                        case '[':
                            state = ParserState.STATE_BRACKET;
                            break;
                        case '{':
                            state = ParserState.STATE_ESCAPE_KEYWORD_START;
                            dest.append(escapedSql.substring(start, lpc));
                            break;
                        case '}':
                            if (depth > 0) {
                                dest.append(escapedSql.substring(start, lpc));
                                return lpc;
                            }
                            throw new SQLSyntaxErrorException("Extraneous closing brace at -- " +
                                    escapedSql.substring(lpc));
                        default:
                            break;
                    }
                    break;
                case STATE_SINGLE_STRING:
                    if (ch == '\'')
                        state = ParserState.STATE_SQL;
                    break;
                case STATE_DOUBLE_STRING:
                    if (ch == '"')
                        state = ParserState.STATE_SQL;
                    break;
                case STATE_BRACKET:
                    if (ch == ']')
                        state = ParserState.STATE_SQL;
                    break;
                case STATE_ESCAPE_KEYWORD_START:
                    if (Character.isWhitespace(ch)) {
                        throw new SQLSyntaxErrorException(
                                "A keyword must immediately follow the start of an escape sequence");
                    }
                    keywordStart = lpc;
                    state = ParserState.STATE_ESCAPE_KEYWORD;
                    break;
                case STATE_ESCAPE_KEYWORD:
                    if (Character.isWhitespace(ch)) {
                        StringBuilder argBuilder = new StringBuilder();
                        String keyword = escapedSql.substring(keywordStart, lpc);

                        lpc = transform(depth + 1, escapedSql, lpc, argBuilder, handlerMap);
                        start = lpc + 1;

                        EscapeHandler handler = handlerMap.get(keyword);

                        if (handler == null) {
                            throw new SQLSyntaxErrorException("Unknown escape keyword -- " + keyword);
                        }

                        dest.append(handler.handle(keyword, argBuilder.toString().trim()));

                        state = ParserState.STATE_SQL;
                    }
                    break;
            }
        }

        if (state != ParserState.STATE_SQL)
            throw new SQLSyntaxErrorException(state.getMessage());

        if (start < escapedSql.length())
            dest.append(escapedSql.substring(start, escapedSql.length()));

        return retval;
    }

    public static String transform(String escapedSql, Map<String, EscapeHandler> handlerMap) throws SQLException {
        StringBuilder dest = new StringBuilder();

        transform(0, escapedSql, 0, dest, handlerMap);

        return dest.toString();
    }
}
