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

package org.sqlite.jdbcng.bridj;

import org.bridj.BridJ;
import org.bridj.Callback;
import org.bridj.Pointer;
import org.bridj.StructObject;
import org.bridj.ann.Library;
import org.bridj.ann.Optional;

import java.sql.*;
import java.util.HashMap;

@Library("sqlite3")
public class Sqlite3 {

    static {
        BridJ.register();
    }

    public static class NoopReleaser implements Pointer.Releaser {
        @Override
        public void release(Pointer<?> objects) {
        }
    }

    public static class StatementReleaser implements Pointer.Releaser {
        @Override
        public void release(Pointer<?> stmt) {
            sqlite3_finalize(stmt.as(Statement.class));
        }
    }

    private static final StatementReleaser STATEMENT_RELEASER = new StatementReleaser();

    public static class DbReleaser implements Pointer.Releaser {

        @Override
        public void release(Pointer<?> db) {
            try {
                sqlite3_close_v2(db.as(Sqlite3Db.class));
            }
            catch (UnsatisfiedLinkError e) {
                sqlite3_close(db.as(Sqlite3Db.class));
            }
        }
    }

    private static final DbReleaser DB_RELEASER = new DbReleaser();

    public static class Sqlite3FreeCallback extends Callback<Sqlite3FreeCallback> {
        public void apply(Pointer<Byte> mem) {
            sqlite3_free(mem);
        }
    }

    public static abstract class BufferDestructorBase extends Callback<BufferDestructorBase> {
        public abstract void apply(Pointer<Void> mem);
    }

    public static class BufferDestructor extends BufferDestructorBase {
        private final Pointer<Byte> buffer;

        public BufferDestructor(Pointer<Byte> buffer) {
            this.buffer = buffer;
        }

        @Override
        public void apply(Pointer< Void > mem) {
            BridJ.unprotectFromGC(this);
        }
    }

    public static abstract class AuthCallbackBase extends Callback<AuthCallbackBase> {
        public abstract int apply(Pointer<Void> context,
                                  int actionCode,
                                  Pointer<Byte> arg1,
                                  Pointer<Byte> arg2,
                                  Pointer<Byte> arg3,
                                  Pointer<Byte> arg4);
    }

    public static class Sqlite3Db extends StructObject {
    }

    public static class Statement extends StructObject {
    }

    public static native Pointer<Byte> sqlite3_libversion();
    public static native int sqlite3_libversion_number();
    public static native Pointer<Byte> sqlite3_sourceid();

    public static native Pointer<Byte> sqlite3_mprintf(Pointer<Byte> fmt, Object... varargs);
    public static native void sqlite3_free(Pointer<Byte> mem);

    public static native int sqlite3_changes(Pointer<Sqlite3Db> db);
    public static native int sqlite3_open(Pointer<Byte> filename, Pointer<Pointer<Sqlite3Db>> db);
    public static native int sqlite3_close(Pointer<Sqlite3Db> db);
    public static native int sqlite3_close_v2(Pointer<Sqlite3Db> db);

    public static native int sqlite3_set_authorizer(Pointer<Sqlite3Db> db,
                                                    Pointer<AuthCallbackBase> cb,
                                                    Pointer<Void> userData);

    public static native int sqlite3_get_autocommit(Pointer<Sqlite3Db> db);
    public static native Pointer<Byte> sqlite3_errmsg(Pointer<Sqlite3Db> db);

    public static native int sqlite3_limit(Pointer<Sqlite3Db> db, int id, int newVal);

    public static native int sqlite3_clear_bindings(Pointer<Statement> stmt);
    public static native int sqlite3_bind_parameter_count(Pointer<Statement> stmt);
    public static native int sqlite3_bind_null(Pointer<Statement> stmt, int arg);
    public static native int sqlite3_bind_blob(Pointer<Statement> stmt, int arg,
                                               Pointer<Byte> mem, int len,
                                               Pointer<BufferDestructorBase> dest);
    public static native int sqlite3_bind_int(Pointer<Statement> stmt, int arg, int value);
    public static native int sqlite3_bind_int64(Pointer<Statement> stmt, int arg, long value);
    public static native int sqlite3_bind_double(Pointer<Statement> stmt, int arg, double value);
    public static native int sqlite3_bind_text(Pointer<Statement> stmt,
                                               int arg,
                                               Pointer<Byte> str,
                                               int len,
                                               Pointer<BufferDestructorBase> dest);

    public static native int sqlite3_prepare_v2(Pointer<Sqlite3Db> db,
                                                Pointer<Byte> sql,
                                                int len,
                                                Pointer<Pointer<Statement>> stmt,
                                                Pointer<Pointer<Byte>> tail);

    public static native int sqlite3_step(Pointer<Statement> stmt);

    public static native int sqlite3_stmt_readonly(Pointer<Statement> stmt);

    public static native int sqlite3_reset(Pointer<Statement> stmt);
    public static native int sqlite3_finalize(Pointer<Statement> stmt);

    public static native int sqlite3_column_count(Pointer<Statement> stmt);
    public static native Pointer<Byte> sqlite3_column_name(Pointer<Statement> stmt, int col);
    public static native Pointer<Byte> sqlite3_column_decltype(Pointer<Statement> stmt, int col);
    public static native int sqlite3_column_type(Pointer<Statement> stmt, int col);

    @Optional
    public static native Pointer<Byte> sqlite3_column_database_name(Pointer<Statement> stmt, int col);
    @Optional
    public static native Pointer<Byte> sqlite3_column_table_name(Pointer<Statement> stmt, int col);
    @Optional
    public static native Pointer<Byte> sqlite3_column_origin_name(Pointer<Statement> stmt, int col);

    public static native Pointer<Byte> sqlite3_column_blob(Pointer<Statement> stmt, int col);
    public static native int sqlite3_column_bytes(Pointer<Statement> stmt, int col);
    public static native Pointer<Byte> sqlite3_column_text(Pointer<Statement> stmt, int col);
    public static native int sqlite3_column_int(Pointer<Statement> stmt, int col);
    public static native long sqlite3_column_int64(Pointer<Statement> stmt, int col);
    public static native double sqlite3_column_double(Pointer<Statement> stmt, int col);

    private static Pointer<BufferDestructorBase> constantFunctionValue(long value) {
        Pointer ptr = Pointer.pointerToAddress(value, 0, new NoopReleaser());

        return ptr.as(BufferDestructorBase.class);
    }

    public static final Pointer<BufferDestructorBase> SQLITE_STATIC = null;
    public static final Pointer<BufferDestructorBase> SQLITE_TRANSIENT = constantFunctionValue(-1);

    public static Pointer<Statement> withReleaser(Pointer<Statement> stmt) {
        try {
            return Pointer.pointerToAddress(stmt.getPeer(), Statement.class, STATEMENT_RELEASER);
        }
        catch (Throwable e) {
            STATEMENT_RELEASER.release(stmt);
            throw e;
        }
    }

    public static Pointer<Sqlite3Db> withDbReleaser(Pointer<Sqlite3Db> db) {
        try {
            if (db == null)
                return null;

            return Pointer.pointerToAddress(db.getPeer(), Sqlite3Db.class, DB_RELEASER);
        }
        catch (Throwable e) {
            DB_RELEASER.release(db);
            throw e;
        }
    }

    public static String mprintf(String fmt, Object... varargs) {
        Object[] xargs = new Object[varargs.length];
        Pointer<Byte> result;

        for (int lpc = 0; lpc < varargs.length; lpc++) {
            if (varargs[lpc] instanceof String) {
                xargs[lpc] = Pointer.pointerToCString((String)varargs[lpc]);
            }
            else {
                xargs[lpc] = varargs[lpc];
            }
        }
        result = sqlite3_mprintf(Pointer.pointerToCString(fmt), xargs);

        try {
            return result.getCString();
        }
        finally {
            sqlite3_free(result);
        }
    }

    public static String join(Object[] elems, String sep) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;

        for (Object elem : elems) {
            if (!first)
                builder.append(sep);
            builder.append(elem.toString());
            first = false;
        }

        return builder.toString();
    }

    public enum ActionCode {
        SQLITE_CREATE_INDEX(1),   /* Index Name      Table Name      */
        SQLITE_CREATE_TABLE(2),   /* Table Name      NULL            */
        SQLITE_CREATE_TEMP_INDEX(3),   /* Index Name      Table Name      */
        SQLITE_CREATE_TEMP_TABLE(4),   /* Table Name      NULL            */
        SQLITE_CREATE_TEMP_TRIGGER(5),   /* Trigger Name    Table Name      */
        SQLITE_CREATE_TEMP_VIEW(6),   /* View Name       NULL            */
        SQLITE_CREATE_TRIGGER(7),   /* Trigger Name    Table Name      */
        SQLITE_CREATE_VIEW(8),   /* View Name       NULL            */
        SQLITE_DELETE(9),   /* Table Name      NULL            */
        SQLITE_DROP_INDEX(10),   /* Index Name      Table Name      */
        SQLITE_DROP_TABLE(11),   /* Table Name      NULL            */
        SQLITE_DROP_TEMP_INDEX(12),   /* Index Name      Table Name      */
        SQLITE_DROP_TEMP_TABLE(13),   /* Table Name      NULL            */
        SQLITE_DROP_TEMP_TRIGGER(14),   /* Trigger Name    Table Name      */
        SQLITE_DROP_TEMP_VIEW(15),   /* View Name       NULL            */
        SQLITE_DROP_TRIGGER(16),   /* Trigger Name    Table Name      */
        SQLITE_DROP_VIEW(17),   /* View Name       NULL            */
        SQLITE_INSERT(18),   /* Table Name      NULL            */
        SQLITE_PRAGMA(19),   /* Pragma Name     1st arg or NULL */
        SQLITE_READ(20),   /* Table Name      Column Name     */
        SQLITE_SELECT(21),   /* NULL            NULL            */
        SQLITE_TRANSACTION(22),   /* Operation       NULL            */
        SQLITE_UPDATE(23),   /* Table Name      Column Name     */
        SQLITE_ATTACH(24),   /* Filename        NULL            */
        SQLITE_DETACH(25),   /* Database Name   NULL            */
        SQLITE_ALTER_TABLE(26),   /* Database Name   Table Name      */
        SQLITE_REINDEX(27),   /* Index Name      NULL            */
        SQLITE_ANALYZE(28),   /* Table Name      NULL            */
        SQLITE_CREATE_VTABLE(29),   /* Table Name      Module Name     */
        SQLITE_DROP_VTABLE(30),   /* Table Name      Module Name     */
        SQLITE_FUNCTION(31),   /* NULL            Function Name   */
        SQLITE_SAVEPOINT(32),   /* Operation       Savepoint Name  */
        SQLITE_COPY(0);   /* No longer used */

        private static final HashMap<Integer, ActionCode> VALUE_TO_ENUM = new HashMap<>();

        static {
            for (ActionCode rc : values()) {
                VALUE_TO_ENUM.put(rc.value, rc);
            }
        }

        public static ActionCode valueOf(int value) {
            return VALUE_TO_ENUM.get(value);
        }

        private final int value;

        ActionCode(int value_in) {
            this.value = value_in;
        }

        public int value() {
            return this.value;
        }
    };

    public enum AuthResult {
        SQLITE_OK(0),
        SQLITE_DENY(1),
        SQLITE_IGNORE(2);

        private static final HashMap<Integer, AuthResult> VALUE_TO_ENUM = new HashMap<>();

        static {
            for (AuthResult rc : values()) {
                VALUE_TO_ENUM.put(rc.value, rc);
            }
        }

        public static AuthResult valueOf(int value) {
            return VALUE_TO_ENUM.get(value);
        }

        private final int value;

        AuthResult(int value_in) {
            this.value = value_in;
        }

        public int value() {
            return this.value;
        }
    }

    public enum Limit {
        SQLITE_LIMIT_LENGTH(0),
        SQLITE_LIMIT_SQL_LENGTH(1),
        SQLITE_LIMIT_COLUMN(2),
        SQLITE_LIMIT_EXPR_DEPTH(3),
        SQLITE_LIMIT_COMPOUND_SELECT(4),
        SQLITE_LIMIT_VDBE_OP(5),
        SQLITE_LIMIT_FUNCTION_ARG(6),
        SQLITE_LIMIT_ATTACHED(7),
        SQLITE_LIMIT_LIKE_PATTERN_LENGTH(8),
        SQLITE_LIMIT_VARIABLE_NUMBER(9),
        SQLITE_LIMIT_TRIGGER_DEPTH(10);

        private static final HashMap<Integer, Limit> VALUE_TO_ENUM = new HashMap<>();

        static {
            for (Limit rc : values()) {
                VALUE_TO_ENUM.put(rc.value, rc);
            }
        }

        public static Limit valueOf(int value) {
            return VALUE_TO_ENUM.get(value);
        }

        private final int value;

        Limit(int value_in) {
            this.value = value_in;
        }

        public int value() {
            return this.value;
        }
    };

    public enum DataType {
        SQLITE_INTEGER(1, "INTEGER"),
        SQLITE_FLOAT(2, "REAL"),
        SQLITE_TEXT(3, "TEXT"),
        SQLITE_BLOB(4, "BLOB"),
        SQLITE_NULL(5, "NULL");

        private static final HashMap<Integer, DataType> VALUE_TO_ENUM = new HashMap<>();

        static {
            for (DataType dt : values()) {
                VALUE_TO_ENUM.put(dt.value, dt);
            }
        }

        public static DataType valueOf(int value) {
            return VALUE_TO_ENUM.get(value);
        }

        private final int value;
        private final String sqlType;

        DataType(int value, String sqlType) {
            this.value = value;
            this.sqlType = sqlType;
        }

        public int value() {
            return this.value;
        }

        public String getSqlType() {
            return this.sqlType;
        }
    }

    public enum ReturnCodes {
        SQLITE_OK(0, "Successful result"),
        SQLITE_ERROR(1, "SQL error or missing database"),
        SQLITE_INTERNAL(2, "Internal logic error in SQLite"),
        SQLITE_PERM(3, "Access permission denied"),
        SQLITE_ABORT(4, "Callback routine requested an abort"),
        SQLITE_BUSY(5, "The database file is locked"),
        SQLITE_LOCKED(6, "A table in the database is locked"),
        SQLITE_NOMEM(7, "A malloc() failed"),
        SQLITE_READONLY(8, "Attempt to write a readonly database"),
        SQLITE_INTERRUPT(9, "Operation terminated by sqlite3_interrupt()"),
        SQLITE_IOERR(10, "Some kind of disk I/O error occurred"),
        SQLITE_CORRUPT(11, "The database disk image is malformed"),
        SQLITE_NOTFOUND(12, "Unknown opcode in sqlite3_file_control()"),
        SQLITE_FULL(13, "Insertion failed because database is full"),
        SQLITE_CANTOPEN(14, "Unable to open the database file"),
        SQLITE_PROTOCOL(15, "Database lock protocol error"),
        SQLITE_EMPTY(16, "Database is empty"),
        SQLITE_SCHEMA(17, "The database schema changed"),
        SQLITE_TOOBIG(18, "String or BLOB exceeds size limit"),
        SQLITE_CONSTRAINT(19, "Abort due to constraint violation"),
        SQLITE_MISMATCH(20, "Data type mismatch"),
        SQLITE_MISUSE(21, "Library used incorrectly"),
        SQLITE_NOLFS(22, "Uses OS features not supported on host"),
        SQLITE_AUTH(23, "Authorization denied"),
        SQLITE_FORMAT(24, "Auxiliary database format error"),
        SQLITE_RANGE(25, "2nd parameter to sqlite3_bind out of range"),
        SQLITE_NOTADB(26, "File opened that is not a database file"),
        SQLITE_ROW(100, "sqlite3_step() has another row ready"),
        SQLITE_DONE(101, "sqlite3_step() has finished executing");

        private static final HashMap<Long, ReturnCodes> VALUE_TO_ENUM = new HashMap<>();

        static {
            for (ReturnCodes rc : values()) {
                VALUE_TO_ENUM.put(rc.value, rc);
            }
        }

        public static ReturnCodes valueOf(long value) {
            return VALUE_TO_ENUM.get(value);
        }

        private final long value;
        private final String msg;

        ReturnCodes(long value_in, String msg) {
            this.value = value_in;
            this.msg = msg;
        }

        public long value() {
            return this.value;
        }

        public String message() {
            return this.msg;
        }
    }

    public static void checkOk(int rc, Pointer<Sqlite3Db> db) throws SQLException {
        ReturnCodes rcEnum = ReturnCodes.valueOf(rc);

        switch (rcEnum){
            case SQLITE_OK:
            case SQLITE_ROW:
            case SQLITE_DONE:
                break;
            default: {
                String msg;

                if (db != null) {
                    msg = sqlite3_errmsg(db).getCString();
                }
                else {
                    msg = rcEnum.message();
                }

                switch (rcEnum) {
                    case SQLITE_ERROR:
                        throw new SQLSyntaxErrorException(msg, "", rc);
                    case SQLITE_AUTH:
                    case SQLITE_CORRUPT:
                        throw new SQLNonTransientException(msg, "", rc);
                    case SQLITE_BUSY:
                    case SQLITE_IOERR:
                    case SQLITE_NOTFOUND:
                    case SQLITE_LOCKED:
                        throw new SQLTransientException(msg, "", rc);
                    case SQLITE_NOTADB:
                        throw new SQLNonTransientConnectionException(msg, "", rc);
                    case SQLITE_CANTOPEN:
                        throw new SQLTransientConnectionException(msg, "", rc);
                    default:
                        throw new SQLException(msg, "", rc);
                }
            }
        }
    }

    public static void checkOk(int rc) throws SQLException {
        checkOk(rc, null);
    }
}
