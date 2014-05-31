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

import org.bridj.Pointer;
import org.junit.Test;
import org.sqlitejdbcng.bridj.Sqlite3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;

public class SqliteConnectionTest extends SqliteTestHelper {
    @Test
    public void testFileType() throws Exception {
        Path path = Paths.get(this.dbFile.getAbsolutePath());
        File blankFile = this.testFolder.newFile("blank");
        File shortFile = this.testFolder.newFile("short");
        File invalidFile = this.testFolder.newFile("invalid");

        assertEquals("application/x-sqlite3", Files.probeContentType(path));
        assertEquals(null, Files.probeContentType(Paths.get(blankFile.toURI())));
        try (OutputStream os = new FileOutputStream(shortFile)) {
            os.write(1);
        }
        assertNotEquals("application/x-sqlite3", Files.probeContentType(Paths.get(shortFile.toURI())));
        try (OutputStream os = new FileOutputStream(invalidFile)) {
            os.write(new byte[32]);
        }
        assertNotEquals("application/x-sqlite3", Files.probeContentType(Paths.get(invalidFile.toURI())));
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
                assertTrue(e.getMessage(), e.getMessage().contains("locked"));
                assertFalse(this.conn.getAutoCommit());
            }
        }

        this.conn.setAutoCommit(true);
        assertTrue(this.conn.getAutoCommit());
    }

    @Test(expected = SQLIntegrityConstraintViolationException.class)
    public void testRollbackException() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_table VALUES (1, 'test')");
        }
    }

    @Test
    public void testCreateStatement() throws Exception {
        Statement stmt = this.conn.createStatement();

        assertFalse(stmt.isClosed());

        ResultSet rs = stmt.executeQuery("SELECT * FROM test_table");
        assertFalse(rs.isClosed());

        this.conn.close();
        assertTrue(rs.isClosed());
        assertTrue(stmt.isClosed());
        this.conn.close();
        assertTrue(this.conn.isClosed());
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

    private static final String[] EXPECTED_WARNING_MSGS = {
            "SQLite only supports TYPE_FORWARD_ONLY result sets",
            "SQLite only supports CONCUR_READ_ONLY result sets",
            "SQLite only supports CLOSE_CURSORS_AT_COMMIT result sets",
    };

    @Test
    public void testWarnings() throws Exception {
        assertNull(this.conn.getWarnings());
        try (PreparedStatement stmt = this.conn.prepareStatement("SELECT * FROM test_table",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE,
                ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
            SQLWarning warnings = this.conn.getWarnings();
            Set<String> msgs = new HashSet<>();

            assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
            assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, stmt.getResultSetHoldability());

            assertNotNull(warnings);
            while (warnings != null) {
                msgs.add(warnings.getMessage());
                warnings = warnings.getNextWarning();
            }

            assertEquals(new HashSet<>(Arrays.asList(EXPECTED_WARNING_MSGS)), msgs);
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareCall() throws Exception {
        this.conn.prepareCall("foo");
    }

    @Test
    public void testRollback() throws Exception {
        this.conn.setAutoCommit(false);

        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_table VALUES (2, 'test')");
            this.conn.rollback();

            assertFalse(this.conn.getAutoCommit());
            this.conn.setAutoCommit(true);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                int count = 0;

                while (rs.next()) {
                    count += 1;
                }
                assertEquals(1, count);
            }
        }
    }

    @Test
    public void testAbort() throws Exception {
        Sqlite3.ProgressCallbackBase delayCallback = new Sqlite3.ProgressCallbackBase() {
            @Override
            public int apply(Pointer<Void> context) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {

                }
                return 0;
            }
        };

        Sqlite3.sqlite3_progress_handler(((SqliteConnection)this.conn).getHandle(), 1, Pointer.pointerTo(delayCallback), null);

        final List<Runnable> commandList = new ArrayList<>();
        final Executor monitor = new Executor() {
            @Override
            public void execute(Runnable command) {
                commandList.add(command);
            }
        };

        Thread aborter = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                    conn.abort(monitor);
                } catch (SQLException e) {

                } catch (InterruptedException e) {

                }
            }
        });
        aborter.setDaemon(true);
        aborter.start();

        boolean reachedExecute = false;

        try (Statement stmt = this.conn.createStatement()) {
            reachedExecute = true;
            stmt.executeUpdate("INSERT INTO test_table VALUES (2, 'test')");
            fail("Statement was not aborted?");
        }
        catch (SQLException e) {
            assertTrue(reachedExecute);
        }

        assertTrue(this.conn.isClosed());

        assertEquals(1, commandList.size());

        Thread waiter = new Thread(commandList.get(0));

        waiter.setDaemon(true);
        waiter.start();

        waiter.join(100);
        assertTrue(waiter.isAlive());
        this.conn.close();
        waiter.join(100);
        assertFalse(waiter.isAlive());
    }

    @Test
    public void testSavepoint() throws Exception {
        try {
            this.conn.setSavepoint();
            fail("Setting a savepoint should fail outside of autocommit");
        }
        catch (SQLException e) {

        }

        this.conn.setAutoCommit(false);

        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO test_table VALUES (2, 'test')");
            Savepoint sp = this.conn.setSavepoint();
            stmt.executeUpdate("INSERT INTO test_table VALUES (3, 'test')");
            try {
                sp.getSavepointName();
                fail("Able to get savepoint name?");
            }
            catch (SQLException e) {
            }
            sp.getSavepointId();
            this.conn.rollback(sp);
            this.conn.commit();

            try {
                this.conn.rollback(sp);
                fail("Rollback should fail on an invalid savepoint");
            }
            catch (SQLException e) {
            }

            try {
                this.conn.releaseSavepoint(sp);
                fail("Release should fail on an invalid savepoint");
            }
            catch (SQLException e) {
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }

            stmt.executeUpdate("INSERT INTO test_table VALUES (3, 'test')");
            sp = this.conn.setSavepoint("test\"'");
            stmt.executeUpdate("INSERT INTO test_table VALUES (4, 'test')");
            try {
                sp.getSavepointId();
                fail("Able to get savepoint id?");
            }
            catch (SQLException e) {
            }
            assertEquals("test\"'", sp.getSavepointName());
            this.conn.rollback(sp);
            this.conn.commit();

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }

            stmt.executeUpdate("INSERT INTO test_table VALUES (4, 'test')");
            sp = this.conn.setSavepoint("test");
            stmt.executeUpdate("INSERT INTO test_table VALUES (5, 'test')");
            this.conn.releaseSavepoint(sp);
            this.conn.commit();

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGetClientInfo() throws Exception {
        assertNull(this.conn.getClientInfo("AppName"));
        assertNotNull(this.conn.getClientInfo());
    }

    @Test
    public void testSetClientInfo() throws Exception {
        Properties props = new Properties();

        props.put("AppName", "Test");
        props.put("Key", "Value");
        try {
            this.conn.setClientInfo(props);
            fail("setClientInfo should not be supported");
        }
        catch (SQLClientInfoException e) {
            assertEquals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY, e.getFailedProperties().get("AppName"));
            assertEquals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY, e.getFailedProperties().get("Key"));
            assertEquals(2, e.getFailedProperties().size());
        }

        try {
            this.conn.setClientInfo("AppName", "Test");
            fail("setClientInfo should not be supported");
        }
        catch (SQLClientInfoException e) {
            assertEquals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY, e.getFailedProperties().get("AppName"));
            assertEquals(1, e.getFailedProperties().size());
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetTypeMap() throws Exception {
        this.conn.getTypeMap();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetTypeMap() throws Exception {
        this.conn.setTypeMap(new HashMap<String, Class<?>>());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateSQLXML() throws Exception {
        this.conn.createSQLXML();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetNetworkTimeout() throws Exception {
        this.conn.setNetworkTimeout(null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetNetworkTimeout() throws Exception {
        this.conn.getNetworkTimeout();
    }

    @Test
    public void testGetMetaData() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();

        assertNotNull(dmd);
        assertEquals(dmd, this.conn.getMetaData());

        this.conn.close();
        try {
            this.conn.getMetaData();
            fail("getMetaData should fail after the DB is closed");
        }
        catch (SQLException e) {

        }
    }
}
