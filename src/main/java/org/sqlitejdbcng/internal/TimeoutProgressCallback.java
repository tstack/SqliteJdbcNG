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

package org.sqlitejdbcng.internal;

import org.bridj.Pointer;
import org.sqlitejdbcng.SqliteConnection;
import org.sqlitejdbcng.SqliteConnectionProgressCallback;

import java.sql.SQLException;

public class TimeoutProgressCallback extends SqliteConnectionProgressCallback {
    private long expirationTime;

    public TimeoutProgressCallback(SqliteConnection conn) {
        super(conn);
    }

    public TimeoutProgressCallback setExpiration(long expirationTime) throws SQLException {
        TimeoutProgressCallback retval = null;

        if (expirationTime == 0) {
            this.expirationTime = expirationTime;
        }
        else {
            this.expirationTime = System.currentTimeMillis() + expirationTime;
            this.conn.pushCallback(this);
            retval = this;
        }

        return retval;
    }

    @Override
    public int apply(Pointer<Void> context) {
        if (other != null && other.apply(context) != 0)
            return 1;
        if (System.currentTimeMillis() > this.expirationTime)
            return 1;

        return 0;
    }

    @Override
    public void close() throws SQLException {
        if (this.expirationTime != 0) {
            this.conn.popCallback();
            this.expirationTime = 0;
        }
    }
}
