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

import org.bridj.Pointer;
import org.junit.Test;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class SqliteStatementTest extends SqliteTestHelper {
    @Test
    public void testExecuteBatch() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            stmt.addBatch("INSERT INTO test_table VALUES (2, 'testing')");
            stmt.addBatch("INSERT INTO test_table VALUES (3, 'testing again')");

            assertArrayEquals(new int[]{1, 1}, stmt.executeBatch());

            assertArrayEquals(new int[0], stmt.executeBatch());

            stmt.addBatch("INSERT INTO test_table VALUES (4, 'testing again too')");
            stmt.addBatch("INSERT INTO test_table VALUES (4, 'testing again too')");
            try {
                stmt.executeBatch();
                fail("executeBatch should not have succeeded");
            }
            catch (BatchUpdateException e) {
            }

            assertArrayEquals(new int[0], stmt.executeBatch());

            final String[] tableDump = {
                    "|1|test|",
                    "|2|testing|",
                    "|3|testing again|",
                    "|4|testing again too|",
            };

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertArrayEquals(tableDump, this.formatResultSet(rs));
            }

            stmt.addBatch("INSERT INTO test_table VALUES (2, 'testing')");
            stmt.clearBatch();
            assertArrayEquals(new int[0], stmt.executeBatch());
        }
    }

    @Test
    public void testCloseOnCompletion() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            assertFalse(stmt.isCloseOnCompletion());

            stmt.closeOnCompletion();
            assertTrue(stmt.isCloseOnCompletion());
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                this.formatResultSet(rs);
            }
            assertTrue(stmt.isClosed());
        }

        try (Statement stmt = this.conn.createStatement()) {
            assertFalse(stmt.isCloseOnCompletion());

            stmt.closeOnCompletion();
            assertTrue(stmt.isCloseOnCompletion());
            assertEquals(1, stmt.executeUpdate("INSERT INTO test_table VALUES (2, 'testing')"));
            assertFalse(stmt.isClosed());
        }
    }

    @Test
    public void testBadExecuteUpdate() throws Exception {
        Logger stmtLogger = driver.getParentLogger();
        LogRecorder myHandler = new LogRecorder();

        stmtLogger.addHandler(myHandler);

        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("SELECT * FROM test_table");

            assertEquals("executeUpdate used with a statement that is returning results -- {0}",
                    myHandler.getRecords().get(0).getMessage());
            assertArrayEquals(new Object[] { "SELECT * FROM test_table" },
                    myHandler.getRecords().get(0).getParameters());
        }

        stmtLogger.removeHandler(myHandler);
    }

    @Test
    public void testQueryTimeout() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            SqliteConnection sqliteConnection = (SqliteConnection)this.conn;

            sqliteConnection.setProgressStep(1);
            sqliteConnection.pushCallback(new SqliteConnectionProgressCallback(sqliteConnection) {
                @Override
                public int apply(Pointer<Void> context) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {

                    }
                    return 0;
                }
            });
            stmt.setQueryTimeout(1);
            assertEquals(1, stmt.getQueryTimeout());

            long startTime = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                rs.next();
                fail("Expected a timeout exception");
            }
            catch (SQLTimeoutException e) {
                long endTime = System.currentTimeMillis();

                if (endTime - startTime < 1000) {
                    fail("Timeout expired early -- " + (endTime - startTime));
                }
            }
        }
    }

    @Test
    public void testMaxRows() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_table VALUES (2, 'testing')");

            assertEquals(0, stmt.getMaxRows());

            stmt.setMaxRows(1);
            assertEquals(1, stmt.getMaxRows());
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertTrue(rs.next());
                assertFalse(rs.next());
            }

            stmt.setMaxRows(4);
            assertEquals(4, stmt.getMaxRows());
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
        }
    }
}
