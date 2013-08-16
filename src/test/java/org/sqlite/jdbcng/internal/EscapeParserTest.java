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

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.sqlite.jdbcng.internal.EscapeParser.transform;

public class EscapeParserTest {
    private final Map<String, EscapeHandler> handlerMap = new HashMap<>();

    @Before
    public void setupHandlerMap() {
        handlerMap.put("limit", new PassthruEscapeHandler(true));
    }

    @Test
    public void testGoodTransforms() throws Exception {
        assertEquals("SELECT * FROM test_tables", transform("SELECT * FROM test_tables", this.handlerMap));
        assertEquals("SELECT * FROM test_tables limit 10",
                transform("SELECT * FROM test_tables {limit 10}", this.handlerMap));
        assertEquals("", transform("", this.handlerMap));
        assertEquals("limit foo", transform("{limit foo}", this.handlerMap));
        assertEquals("foo '{bar baz}'", transform("foo '{bar baz}'", this.handlerMap));
        assertEquals("foo \"{bar baz}\"", transform("foo \"{bar baz}\"", this.handlerMap));
        assertEquals("foo [{bar baz}]", transform("foo [{bar baz}]", this.handlerMap));
        assertEquals("limit limit 100", transform("{limit {limit 100}}", this.handlerMap));
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testUnterminatedEscape() throws Exception {
        transform("{foo bar", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testUnterminatedSingle() throws Exception {
        transform("'foo bar", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testUnterminatedDouble() throws Exception {
        transform("\"foo bar", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testUnopenedEscape() throws Exception {
        transform("foo bar}", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testLeadingCurly() throws Exception {
        transform("}", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testUnknownKeyword() throws Exception {
        transform("{foo bar}", this.handlerMap);
    }

    @Test(expected = SQLSyntaxErrorException.class)
    public void testNoKeyword() throws Exception {
        transform("{ bar}", this.handlerMap);
    }
}
