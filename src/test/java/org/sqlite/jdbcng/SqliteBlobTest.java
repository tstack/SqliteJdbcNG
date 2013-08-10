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

import junit.framework.TestCase;

import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;

public class SqliteBlobTest extends TestCase {
    private final byte[] TEST_BYTES = "Hello, World!\n".getBytes();

    public void testSetBytes() throws Exception {
        byte[] buffer = new byte[1024];
        Blob blob = new SqliteBlob();

        assertEquals(0, blob.length());

        blob.setBytes(1, TEST_BYTES);
        assertEquals(TEST_BYTES.length, blob.length());
        assertTrue(Arrays.equals(TEST_BYTES, blob.getBytes(1, (int) blob.length())));
        assertTrue(Arrays.equals(TEST_BYTES, blob.getBytes(1, 1024)));

        blob.truncate(5);
        assertEquals(5, blob.length());
        assertTrue(Arrays.equals(Arrays.copyOf(TEST_BYTES, 5), blob.getBytes(1, 1024)));

        InputStream is = blob.getBinaryStream();
        for (int lpc = 1; lpc <= 5; lpc++) {
            assertEquals(TEST_BYTES[lpc - 1], is.read());
        }
        assertEquals(-1, is.read());
        assertEquals(0, is.read(buffer));

        is = blob.getBinaryStream();
        assertEquals(5, is.skip(1024));
        assertEquals(-1, is.read());

        blob.free();
        blob.free();

        try {
            blob.length();
            fail("length() should fail after blob was freed");
        }
        catch (SQLException e) {
        }
    }

    public void testBlobInQueries() throws Exception {
        SqliteDriver driver = new SqliteDriver();
        Connection conn = driver.connect("jdbc:sqlite:", null);
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS blob_test (b1 BLOB);");

        PreparedStatement ps = conn.prepareStatement("INSERT INTO blob_test VALUES (?)");

        Blob blob = conn.createBlob();

        blob.setBytes(1, TEST_BYTES);
        ps.setBlob(1, blob);
        ps.executeUpdate();

        ResultSet rs = stmt.executeQuery("SELECT * FROM blob_test");

        rs.next();
        blob = rs.getBlob(1);
        assertEquals(TEST_BYTES.length, blob.length());
        assertTrue(Arrays.equals(TEST_BYTES, blob.getBytes(1, 1024)));

        rs.next();
        try {
            blob.length();
            fail("length() should fail after blob was freed by next()");
        }
        catch (SQLException e) {
        }
    }
}
