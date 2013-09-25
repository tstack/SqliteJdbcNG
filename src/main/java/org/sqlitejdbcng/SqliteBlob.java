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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;

public class SqliteBlob implements Blob {
    private Pointer<Byte> ptr;
    private boolean shared;
    private boolean freed;

    public SqliteBlob(Pointer<Byte> ptr) {
        this.ptr = ptr;
    }

    public SqliteBlob() {
        this(null);
    }

    Pointer<Byte> getHandle() {
        this.shared = true;
        return this.ptr;
    }

    void requireBacking() throws SQLException {
        if (this.freed) {
            throw new SQLNonTransientException("Blobs cannot be used after having been freed");
        }
    }

    long requireValidPos(long pos) throws SQLException {
        if (pos < 1) {
            throw new SQLNonTransientException("Position must be greater than zero");
        }

        return pos - 1;
    }

    long requireValidPos(long pos, long len) throws SQLException {
        if ((pos - 1) + len >= this.length()) {
            throw new SQLNonTransientException("Position must be less than " + this.length());
        }

        return this.requireValidPos(pos);
    }

    long requireValidLength(long length) throws SQLException {
        if (length < 0) {
            throw new SQLNonTransientException("Length must be a positive value");
        }

        return length;
    }

    @Override
    public long length() throws SQLException {
        requireBacking();

        if (this.ptr == null)
            return 0;

        return this.ptr.getValidBytes();
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        requireBacking();

        pos = requireValidPos(pos);

        long actualLength = Math.min(this.length() - pos, requireValidLength(length) - pos);
        byte[] retval = new byte[(int)actualLength];

        this.ptr.getBytesAtOffset(pos, retval, 0, (int)actualLength);

        return retval;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return this.getBinaryStream(1, this.length());
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        requireBacking();

        Pointer<Byte> needle = Pointer.pointerToBytes(pattern);
        Pointer<Byte> loc = this.ptr.offset(requireValidPos(start, pattern.length)).find(needle);

        if (loc == null) {
            return -1;
        }

        return loc.getPeer() - this.ptr.getPeer() + 1;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        requireBacking();

        SqliteBlob sb = (SqliteBlob)pattern;
        Pointer<Byte> loc = this.ptr.offset(requireValidPos(start, pattern.length())).find(sb.ptr);

        if (loc == null) {
            return -1;
        }

        return loc.getPeer() - this.ptr.getPeer() + 1;
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return this.setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        requireBacking();

        long actualLength = requireValidPos(pos) + requireValidLength(len);

        if (offset < 0) {
            throw new SQLNonTransientException("offset must be a positive value");
        }

        if (actualLength > this.length()) {
            this.truncate(actualLength);
        }

        this.ptr.setBytesAtOffset(pos - 1, bytes, offset, len);

        return len;
    }

    @Override
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        requireBacking();
        requireValidPos(pos);

        return new OutputStream() {
            private long streamPos = pos;

            @Override
            public void write(int b) throws IOException {
                try {
                    setBytes(this.streamPos, new byte[] { (byte)b });

                    this.streamPos += 1;
                }
                catch (SQLException e) {
                    throw new IOException("Unable to write to Blob", e);
                }
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                try {
                    setBytes(this.streamPos, b, off, (int)requireValidLength(len));

                    this.streamPos += len;
                }
                catch (SQLException e) {
                    throw new IOException("Unable to write to Blob", e);
                }
            }
        };
    }

    @Override
    public void truncate(long len) throws SQLException {
        requireBacking();

        Pointer<Byte> newPtr = Pointer.allocateBytes(requireValidLength(len));

        if (this.ptr != null) {
            this.ptr.copyTo(newPtr, len);
        }
        this.ptr = newPtr;
    }

    @Override
    public void free() throws SQLException {
        if (!this.shared) {
            Pointer.release(this.ptr);
        }
        this.ptr = null;
        this.freed = true;
    }

    @Override
    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        requireBacking();
        requireValidPos(pos);
        requireValidLength(length);

        return new InputStream() {
            private final long upperLength = Math.min(length() - (pos - 1), length - (pos - 1));
            private long markPos = pos;
            private long streamPos = pos;

            @Override
            public int available() throws IOException {
                return (int)(upperLength - (this.streamPos - 1));
            }

            @Override
            public synchronized void mark(int readlimit) {
                this.markPos = this.streamPos;
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public synchronized void reset() throws IOException {
                this.streamPos = this.markPos;
            }

            @Override
            public long skip(long n) throws IOException {
                if (n < 0)
                    n = 0;

                long retval = Math.min(this.upperLength + 1, this.streamPos + n);

                try {
                    return retval - this.streamPos;
                }
                finally {
                    this.streamPos = retval;
                }
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int actualLength = (int)Math.min(this.upperLength - (this.streamPos - 1), len);

                ptr.getBytesAtOffset(this.streamPos - 1, b, off, actualLength);

                this.streamPos += actualLength;

                return actualLength;
            }

            @Override
            public int read() throws IOException {
                int retval;

                if (this.streamPos <= this.upperLength) {
                    retval = ptr.offset(this.streamPos - 1).getByte();

                    this.streamPos += 1;
                }
                else {
                    retval = -1;
                }

                return retval;
            }
        };
    }
}
