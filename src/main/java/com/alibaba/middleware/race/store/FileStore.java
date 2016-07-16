package com.alibaba.middleware.race.store;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileStore {

    private String name;
    private FileIO  file;
    private long filePos;
    private long fileLength;

    protected FileStore(String name, String mode) {
        this.name = name;
        try {
            boolean exists = FilePath.exists(name);
            if (exists && !FilePath.isWritable(name)) {
                mode = "r";
            } else {
                FilePath.createFile(name);
            }
            file = new FileIO(name, mode);
            if (exists) {
                fileLength = file.size();
            }
        } catch (IOException e) {
            System.err.println("ERROR: name: " + name + " mode: " + mode + "," + e);
        }
    }

    public static FileStore open(String name, String mode) {
        return new FileStore(name, mode);
    }

    public void close() {
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                System.err.println("close file error:" + name);
            } finally {
                file = null;
            }
        }
    }

    public void read(byte[] b, int off, int len) {
        try {
            file.readFully(ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
        }
        filePos += len;
    }

    public void write(byte[] b, int off, int len) {
        try {
            file.writeFully(ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
        }
        filePos += len;
        fileLength = Math.max(filePos, fileLength);
    }

    public void setLength(long newLength) {
        try {
            if (newLength > fileLength) {
                long pos = filePos;
                file.position(newLength - 1);
                file.writeFully(ByteBuffer.wrap(new byte[1]));
                file.position(pos);
            } else {
                file.truncate(newLength);
            }
            fileLength = newLength;
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
        }
    }

    public long getLength() {
        return fileLength;
    }

    public void seek(long pos) {
        try {
            if (pos != filePos) {
                file.position(pos);
                filePos = pos;
            }
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
        }
    }

    public long getFilePointer() {
        return filePos;
    }
}
