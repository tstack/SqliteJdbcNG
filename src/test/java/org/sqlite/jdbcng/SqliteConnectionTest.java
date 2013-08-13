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

package org.sqlite.jdbcng;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.*;

import static org.junit.Assert.*;

public class SqliteConnectionTest {
    private static final SqliteDriver driver = new SqliteDriver();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File dbFile;
    private Connection conn;

    @Before
    public void openConnection() throws Exception {
        this.dbFile = this.testFolder.newFile("test.db");
        this.conn = driver.connect("jdbc:sqlite:" + this.dbFile.getAbsolutePath(), null);
        try (Statement stmt = this.conn.createStatement()) {
            stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)");
            stmt.execute("INSERT INTO test_table VALUES (1, 'test')");
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

    @Test
    public void testTransactionIsolation() throws Exception {
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, this.conn.getTransactionIsolation());
        this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, this.conn.getTransactionIsolation());
        this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, this.conn.getTransactionIsolation());
    }

    @Test(expected = SQLException.class)
    public void testTransactionIsolationOnClosedDB() throws Exception {
        this.conn.close();
        this.conn.getTransactionIsolation();
    }

    @Test(expected = SQLException.class)
    public void testBadTransactionIsolation1() throws Exception {
        this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test(expected = SQLException.class)
    public void testBadTransactionIsolation2() throws Exception {
        this.conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Test(expected = SQLException.class)
    public void testBadTransactionIsolation3() throws Exception {
        this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }

    @Test
    public void testAutoCommit() throws Exception {
        assertTrue(this.conn.getAutoCommit());

        try {
            this.conn.commit();
            fail("commit should throw an exception since we're in auto-commit mode");
        }
        catch (SQLException e) {
        }

        try {
            this.conn.rollback();
            fail("rollback should throw an exception since we're in auto-commit mode");
        }
        catch (SQLException e) {
        }

        this.conn.setAutoCommit(false);
        assertFalse(this.conn.getAutoCommit());

        this.conn.setAutoCommit(false);
        assertFalse(this.conn.getAutoCommit());

        this.conn.commit();
        assertFalse(this.conn.getAutoCommit());

        this.conn.rollback();
        assertFalse(this.conn.getAutoCommit());

        try (Connection conn2 = driver.connect("jdbc:sqlite:" + this.dbFile.getAbsolutePath(), null)) {
            conn2.setAutoCommit(false);

            boolean reachedCommit = false;

            try (Statement stmt = this.conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                    while (rs.next()) {
                        rs.getString(2);
                    }
                }

                try (Statement stmt2 = conn2.createStatement()) {
                    stmt2.executeUpdate("INSERT INTO test_table VALUES (2, 'test2')");
                    reachedCommit = true;
                    conn2.commit();
                }
                fail("Insert should fail with a collision error");
            }
            catch (SQLException e) {
                assertTrue(reachedCommit);
                assertTrue(e.getMessage().contains("database is locked"));
                assertFalse(this.conn.getAutoCommit());
            }
        }

        this.conn.setAutoCommit(true);
        assertTrue(this.conn.getAutoCommit());
    }

    @Test
    public void testCreateStatement() throws Exception {
        Statement stmt = this.conn.createStatement();

        assertFalse(stmt.isClosed());
        this.conn.close();
        assertTrue(stmt.isClosed());
    }

    @Test
    public void testReadOnly() throws Exception {
        assertFalse(this.conn.isReadOnly());

        this.conn.setAutoCommit(false);

        try {
            this.conn.setReadOnly(true);
            fail("Able to set read-only mode when in a transaction?");
        }
        catch (SQLException e) {
        }

        this.conn.setAutoCommit(true);
        this.conn.setReadOnly(true);

        try (Statement stmt = this.conn.createStatement()) {
            final String[] sqlStatements = new String[] {
                    "INSERT INTO test_table VALUES (3, 'test')",
                    "CREATE TABLE foo (id INTEGER)",
                    "DROP TABLE test_table",
                    "PRAGMA synchronous = 1",
            };

            for (String sql : sqlStatements) {
                try {
                    stmt.executeUpdate(sql);
                    fail("Database modification should fail when in read-only mode");
                }
                catch (SQLNonTransientException e) {
                    assertTrue(e.getMessage().contains("not authorized"));
                }
            }

            ResultSet rs = stmt.executeQuery("SELECT * FROM test_table");

            while (rs.next()) {
                rs.getString(2);
            }

            stmt.execute("PRAGMA collation_list");
        }

        this.conn.setReadOnly(true);
        assertTrue(this.conn.isReadOnly());

        this.conn.setReadOnly(false);
        assertFalse(this.conn.isReadOnly());

        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_table VALUES (3, 'test')");
        }
    }

    @Test
    public void testSetCatalog() throws Exception {
        assertNull(this.conn.getWarnings());
        this.conn.setCatalog("foo");
        assertNotNull(this.conn.getWarnings());
    }
}
