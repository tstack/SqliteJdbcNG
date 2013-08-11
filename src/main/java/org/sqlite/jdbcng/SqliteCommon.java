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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Logger;

public class SqliteCommon {
    private static final Logger LOGGER = Logger.getLogger(SqliteCommon.class.getName());

    // SimpleDateFormat is not thread-safe, so give one to each thread
    protected static final ThreadLocal<Calendar> DEFAULT_CALENDAR = new ThreadLocal<Calendar>() {
        @Override
        protected Calendar initialValue()
        {
            return new GregorianCalendar();
        }
    };

    // SimpleDateFormat is not thread-safe, so give one to each thread
    protected static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    // SimpleDateFormat is not thread-safe, so give one to each thread
    protected static final ThreadLocal<SimpleDateFormat> TIME_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("HH:mm:ss");
        }
    };

    // SimpleDateFormat is not thread-safe, so give one to each thread
    protected static final ThreadLocal<SimpleDateFormat> TS_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue()
        {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    protected SQLWarning warnings;

    synchronized void addWarning(SQLWarning warning) {
        LOGGER.warning(warning.getMessage());

        if (this.warnings != null) {
            this.warnings.setNextWarning(warning);
        }
        this.warnings = warning;
    }

    public synchronized SQLWarning getWarnings() throws SQLException {
        return this.warnings;
    }

    public synchronized void clearWarnings() throws SQLException {
        this.warnings = null;
    }
}
