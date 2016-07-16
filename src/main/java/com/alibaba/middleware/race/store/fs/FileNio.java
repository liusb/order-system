package com.alibaba.middleware.race.store.fs;

import com.alibaba.middleware.race.store.fs.FileBase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileNio extends FileBase {
    private FileChannel channel;

    public FileNio(String name, String mode) throws FileNotFoundException {
        channel = new RandomAccessFile(name, mode).getChannel();
    }

    @Override
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public long position() {
        try {
            return channel.position();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public FileBase position(long newPosition) {
        try {
            channel.position(newPosition);
            return this;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int read(ByteBuffer dst) {
        try {
            return channel.read(dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int write(ByteBuffer src) {
        try {
            return channel.write(src);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
