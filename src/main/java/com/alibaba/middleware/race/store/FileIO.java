package com.alibaba.middleware.race.store;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

public class FileIO {
    private final RandomAccessFile file;

    FileIO(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
    }

    public void close() throws IOException {
        this.file.close();
    }

    public long position() throws IOException {
        return file.getFilePointer();
    }

    public void position(long pos) throws IOException {
        file.seek(pos);
    }

    public long size() throws IOException {
        return file.length();
    }

    public void truncate(long newLength) throws IOException {
        if (newLength < file.length()) {
            file.setLength(newLength);
        }
    }

    public synchronized FileLock tryLock(long position, long size,
                                         boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

    public int read(ByteBuffer dst) throws IOException {
        int len = file.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
        }
        return len;
    }

    public void readFully(ByteBuffer dst) throws IOException {
        do {
            int r = read(dst);
            if (r < 0) {
                throw new EOFException();
            }
        } while (dst.remaining() > 0);
    }

    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        file.write(src.array(), src.arrayOffset() + src.position(), len);
        src.position(src.position() + len);
        return len;
    }

    public void writeFully(ByteBuffer src) throws IOException {
        do {
            write(src);
        } while (src.remaining() > 0);
    }
}
