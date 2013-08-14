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

import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.*;

public class SqliteDatabaseMetadataTest extends SqliteTestHelper {
    @Test
    public void testGetSQLKeywords() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();
        String[] words = dmd.getSQLKeywords().split(",");

        assertEquals(words[0], "ABORT");
    }

    private Object[][] getMethodResults() {
        return new Object[][] {
                { "allProceduresAreCallable", false },
                { "allTablesAreSelectable", true },
                { "getURL", "jdbc:sqlite:" + this.dbFile.getAbsolutePath() },
                { "getUserName", "" },
                { "nullsAreSortedHigh", false },
                { "nullsAreSortedLow", true },
                { "nullsAreSortedAtStart", false },
                { "nullsAreSortedAtEnd", false },
                { "getDatabaseProductName", "SQLite" },
                { "getDriverName", "org.sqlite.jdbcng" },
                { "getDriverVersion", "" + SqliteDriver.VERSION[0] + "." + SqliteDriver.VERSION[1] },
                { "getDriverMajorVersion", SqliteDriver.VERSION[0] },
                { "getDriverMinorVersion", SqliteDriver.VERSION[1] },
                { "usesLocalFiles", true },
                { "usesLocalFilePerTable", false },
                { "supportsMixedCaseIdentifiers", false },
                { "storesUpperCaseIdentifiers", false },
                { "storesLowerCaseIdentifiers", false },
                { "storesMixedCaseIdentifiers", true },
        };
    }

    @Test
    public void testFlags() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();
        Class<DatabaseMetaData> cl = DatabaseMetaData.class;

        for (Object[] pair : getMethodResults()) {
            Method method = cl.getMethod((String)pair[0]);
            Object result = method.invoke(dmd);

            assertEquals("Test of -- " + pair[0], pair[1], result);
        }
    }

    @Test
    public void testVersion() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();

        assertTrue(dmd.getDatabaseProductVersion().matches("\\d+\\.\\d+\\.\\d+"));
    }

    @Test
    public void testGetCatalogs() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();

        try (ResultSet rs = dmd.getCatalogs()) {
            assertTrue(rs.next());
            assertEquals("main", rs.getString(1));
            assertEquals("main", rs.getString("TABLE_CAT"));
            assertFalse(rs.next());
        }

        try (Statement stmt = this.conn.createStatement()) {
            stmt.execute("ATTACH ':memory:' as extra_db");

            try (ResultSet rs = dmd.getCatalogs()) {
                assertTrue(rs.next());
                assertEquals("main", rs.getString(1));
                assertEquals("main", rs.getString("TABLE_CAT"));
                assertTrue(rs.next());
                assertEquals("extra_db", rs.getString(1));
                assertEquals("extra_db", rs.getString("TABLE_CAT"));
                assertFalse(rs.next());
            }
        }
    }

    private static final String TABLE_DUMP_HEADER =
            "|TABLE_CAT|TABLE_SCHEM|TABLE_NAME|TABLE_TYPE|REMARKS|TYPE_CAT|TYPE_SCHEM|TYPE_NAME|SELF_REFERENCING_COL_NAME|REF_GENERATION|";

    private static final String[] TABLE_DUMPS = {
            "|main|null|test_table|TABLE|CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)|null|null|null|row_id|SYSTEM|",
            "|main|null|type_table|TABLE|CREATE TABLE type_table (name VARCHAR PRIMARY KEY, birthdate DATETIME, height REAL, eyes INTEGER)|null|null|null|row_id|SYSTEM|",
    };

    @Test
    public void testGetTables() throws Exception {
        DatabaseMetaData dmd = this.conn.getMetaData();

        try (ResultSet rs = dmd.getTables(null, null, "%", null)) {
            ResultSetMetaData rsm = rs.getMetaData();
            String header = "|";

            for (int lpc = 1; lpc <= rsm.getColumnCount(); lpc++) {
                header += rsm.getColumnLabel(lpc) + "|";
            }
            assertEquals(TABLE_DUMP_HEADER, header);

            while (rs.next()) {
                String dump = "|";

                for (int lpc = 1; lpc <= rsm.getColumnCount(); lpc++) {
                    dump += rs.getString(lpc) + "|";
                }

                assertEquals(TABLE_DUMPS[rs.getRow()], dump);
            }
        }

        try (ResultSet rs = dmd.getTables(null, null, "foo", null)) {
            assertFalse(rs.next());
        }

        try (ResultSet rs = dmd.getTables(null, null, "test_%", null)) {
            ResultSetMetaData rsm = rs.getMetaData();

            assertTrue(rs.next());

            String dump = "|";

            for (int lpc = 1; lpc <= rsm.getColumnCount(); lpc++) {
                dump += rs.getString(lpc) + "|";
            }

            assertEquals(TABLE_DUMPS[rs.getRow()], dump);
            assertFalse(rs.next());
        }

    }
}
