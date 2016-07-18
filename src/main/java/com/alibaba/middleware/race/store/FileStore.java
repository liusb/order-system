package com.alibaba.middleware.race.store;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileStore {

    private String name;
    private FileIO file;
    private long posInFile;
    private long fileLength;

    protected FileStore(String name, String mode) {
        this.name = name;
        try {
            boolean exists = FilePath.exists(name);
            if (!exists) {
                FilePath.createFile(name);
            }
            file = new FileIO(name, mode);
            if (exists) {
                fileLength = file.size();
            }
            posInFile = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            e.printStackTrace();
        }
        posInFile += len;
    }

    public void write(byte[] b, int off, int len) {
        try {
            file.writeFully(ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            e.printStackTrace();
        }
        posInFile += len;
        fileLength = Math.max(posInFile, fileLength);
    }

    public void setLength(long newLength) {
        try {
            if (newLength > fileLength) {
                long pos = posInFile;
                file.position(newLength - 1);
                file.writeFully(ByteBuffer.wrap(new byte[1]));
                file.position(pos);
            } else {
                file.truncate(newLength);
                // throw new RuntimeException("不支持缩短文件");
            }
            fileLength = newLength;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getLength() {
        return fileLength;
    }

    public void seek(long pos) {
        try {
            if (pos != posInFile) {
                file.position(pos);
                posInFile = pos;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
