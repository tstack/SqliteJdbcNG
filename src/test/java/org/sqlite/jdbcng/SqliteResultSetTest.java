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

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.Assert.*;

public class SqliteResultSetTest extends SqliteTestHelper {
    @Test
    public void testClose() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM test_table");

            assertEquals(stmt, rs.getStatement());

            assertFalse(rs.isClosed());
            rs.next();
            rs.close();
            assertTrue(rs.isClosed());
            rs.close();
            assertTrue(rs.isClosed());
            try {
                rs.getInt("name");
                fail("able to get data out of a closed result set?");
            }
            catch (SQLException e) {

            }
        }
    }

    @Test
    public void testBadIndex() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                try {
                    rs.getString(0);
                    fail("zero should be an invalid index");
                }
                catch (SQLException e) {

                }
                try {
                    rs.getString(3);
                    fail("three should be out-of-range of the result set");
                }
                catch (SQLException e) {

                }
            }
        }
    }

    @Test
    public void testWasNull() throws Exception {
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO type_table (name, birthdate) VALUES ('test', null)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM type_table")) {
                assertTrue(rs.next());
                assertNull(rs.getDate("birthdate"));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testGetDate() throws Exception {
        long testDate = 1376636400L * 1000L;

        try (PreparedStatement ps = this.conn.prepareStatement(
                "INSERT INTO type_table (name, birthdate) VALUES (?, ?)")) {
            ps.setString(1, "d1");
            ps.setDate(2, new Date(testDate));
            ps.executeUpdate();
        }

        try (Statement stmt = this.conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM type_table")) {
                assertTrue(rs.next());
                assertEquals(testDate, rs.getDate("birthdate").getTime());
                assertNull(rs.getDate(3));
                assertTrue(rs.wasNull());

                try {
                    rs.getDate(1);
                    fail("able to read an invalid date?");
                }
                catch (SQLDataException e) {

                }
            }
        }
    }

    @Test
    public void testGetTime() throws Exception {
        try (PreparedStatement ps = this.conn.prepareStatement(
                "INSERT INTO type_table (name, birthdate) VALUES (?, ?)")) {
            ps.setString(1, "d1");
            ps.setString(2, "05:25");
            ps.executeUpdate();
            ps.setString(1, "d2");
            ps.setString(2, "05:25:22");
            ps.executeUpdate();
            ps.setString(1, "d3");
            ps.setString(2, "05:25:44.123");
            ps.executeUpdate();
        }

        try (Statement stmt = this.conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM type_table")) {
                assertTrue(rs.next());
                assertEquals("05:25:00", rs.getTime("birthdate").toString());
                assertTrue(rs.next());
                assertEquals("05:25:22", rs.getTime(2).toString());
                assertTrue(rs.next());
                assertEquals("05:25:44", rs.getTime(2).toString());
                assertEquals(123, rs.getTime(2).getTime() % 1000);

                assertNull(rs.getTime(3));
                assertTrue(rs.wasNull());

                try {
                    rs.getTime(1);
                    fail("able to read an invalid time?");
                }
                catch (SQLDataException e) {
                    
                }

                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBigDecimal() throws Exception {
        BigDecimal large = new BigDecimal("1" + new String(new char[200]).replace('\0', '0'));
        BigDecimal elarge = new BigDecimal("1.0e+200");

        try (PreparedStatement ps = this.conn.prepareStatement(
                "INSERT INTO type_table (name, width) VALUES (?, ?)")) {
            ps.setString(1, "test1");
            ps.setBigDecimal(2, large);
            ps.executeUpdate();

            ps.setString(1, "test2");
            ps.setString(2, "bad-data");
            ps.executeUpdate();
        }

        try (Statement stmt = this.conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT name, width, height FROM type_table")) {
                BigDecimal decimal;

                assertTrue(rs.next());

                decimal = rs.getBigDecimal("width");
                assertFalse(large.equals(decimal));
                assertEquals(0, large.compareTo(decimal));
                assertEquals(elarge, decimal);

                assertTrue(rs.next());
                try {
                    rs.getBigDecimal(2);
                    fail("Able to read a bad decimal?");
                }
                catch (SQLDataException e) {

                }

                assertNull(rs.getBigDecimal(3));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testPrimitives() throws Exception {
        final double DELTA = 0.00001;

        try (PreparedStatement ps = this.conn.prepareStatement("INSERT INTO prim_table VALUES (?, ?, ?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setBoolean(2, true);
            ps.setLong(3, Long.MAX_VALUE - 1);
            ps.setFloat(4, (float) 123.456789);
            ps.setDouble(5, 12345.6789);
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setBoolean(2, false);
            ps.setNull(3, Types.BIGINT);
            ps.setNull(4, Types.FLOAT);
            ps.setNull(5, Types.DOUBLE);
            ps.executeUpdate();
        }

        try (Statement stmt = this.conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM prim_table")) {
                assertTrue(rs.next());

                assertTrue(rs.getBoolean("b"));
                assertEquals(Long.MAX_VALUE - 1, rs.getLong("bi"));
                assertEquals((float) 123.456789, rs.getFloat("f"), DELTA);
                assertEquals(12345.6789, rs.getDouble("d"), DELTA);

                assertTrue(rs.next());

                assertFalse(rs.getBoolean(2));
                rs.getLong("bi");
                assertTrue(rs.wasNull());
                rs.getFloat("f");
                assertTrue(rs.wasNull());
                rs.getLong("d");
                assertTrue(rs.wasNull());

                assertFalse(rs.next());
            }
        }
    }
}
