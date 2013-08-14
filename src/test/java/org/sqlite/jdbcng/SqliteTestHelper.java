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
 * THIS SOFTWARE IS PROVIDED BY Tim Stack AND CONTRIBUTORS ''AS IS'' AND ANY
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

package org.sqlite.jdbcng;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SqliteTestHelper {
    protected static final SqliteDriver driver = new SqliteDriver();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    protected File dbFile;
    protected Connection conn;

    @Before
    public void openConnection() throws Exception {
        this.dbFile = this.testFolder.newFile("test.db");
        this.conn = driver.connect("jdbc:sqlite:" + this.dbFile.getAbsolutePath(), null);
        try (Statement stmt = this.conn.createStatement()) {
            stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)");
            stmt.execute("INSERT INTO test_table VALUES (1, 'test')");

            stmt.execute("CREATE TABLE type_table (name VARCHAR PRIMARY KEY, birthdate DATETIME, height REAL, eyes INTEGER)");
        }
    }

    @After
    public void closeConnection() throws SQLException {
        if (this.conn != null)
            this.conn.close();
    }

    @Test
    public void testIsValid() throws Exception {
        assertTrue(this.conn.isValid(0));
        this.conn.close();
        assertFalse(this.conn.isValid(0));
        assertTrue(this.conn.isClosed());
    }

}
