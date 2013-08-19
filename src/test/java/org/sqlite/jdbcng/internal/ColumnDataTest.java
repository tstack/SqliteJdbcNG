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

import org.junit.Test;
import org.sqlite.jdbcng.SqliteTestHelper;

import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class ColumnDataTest extends SqliteTestHelper {
    private ColumnData build(String fullType) {
        ColumnData retval = new ColumnData(this.sqliteConnection.getHandle(),
                "main",
                "test_table",
                "col1",
                1,
                "",
                fullType,
                ResultSetMetaData.columnNoNulls,
                0,
                false);

        return retval;
    }

    @Test
    public void testConstructor() {
        ColumnData cd;

        cd = this.build("DECIMAL(");
        assertEquals(Types.VARCHAR, cd.sqlType);
        assertEquals("VARCHAR", cd.type);

        cd = this.build("DECIMAL ( 10 , 2 )");
        assertEquals("DECIMAL", cd.type);
        assertEquals(Types.DECIMAL, cd.sqlType);
        assertEquals(10, cd.precision);
        assertEquals(2, cd.scale);

        cd = this.build("DECIMAL ( 10 )");
        assertEquals("DECIMAL", cd.type);
        assertEquals(Types.DECIMAL, cd.sqlType);
        assertEquals(10, cd.precision);
        assertEquals(0, cd.scale);

        cd = this.build("MEDIUMINT ( 10 )");
        assertEquals("MEDIUMINT", cd.type);
        assertEquals(Types.INTEGER, cd.sqlType);
        assertEquals(10, cd.precision);
        assertEquals(0, cd.scale);

        cd = this.build("BLAH");
        assertEquals("BLAH", cd.type);
        assertEquals(Types.VARCHAR, cd.sqlType);
    }
}
