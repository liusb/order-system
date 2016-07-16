package com.alibaba.middleware.race.store.fs;

import java.nio.ByteBuffer;

public abstract class FileBase {

    public abstract long size();

    public abstract long position();

    public abstract FileBase position(long newPosition);

    public abstract int read(ByteBuffer dst);

    public abstract int write(ByteBuffer src);

    public synchronized int read(ByteBuffer dst, long position) {
        long oldPos = position();
        position(position);
        int len = read(dst);
        position(oldPos);
        return len;
    }

    public synchronized int write(ByteBuffer src, long position) {
        long oldPos = position();
        position(position);
        int len = write(src);
        position(oldPos);
        return len;
    }

}
