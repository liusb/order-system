package com.alibaba.middleware.race.cache;


public class IndexEntry implements Comparable<IndexEntry> {
    final private int fileIdAndLength;
    final private int offset;

    public IndexEntry(byte fileId, int offset, int length) {
        this.fileIdAndLength = (length<<8)|fileId;
        this.offset = offset;
    }

    public byte getFileId() {
        return (byte)(fileIdAndLength&0xff);
    }

    public long getAddress() {
        return 0xffffffffL&offset;
    }

    public int getLength() {
        return fileIdAndLength>>8;
    }

    @Override
    public int compareTo(IndexEntry o) {
        int thisFileId = fileIdAndLength&0xff;
        int thatFIleId = o.fileIdAndLength&0xff;
        if (thisFileId == thatFIleId) {
            return offset < o.offset ? -1: 1;
        } else {
            return thisFileId < thatFIleId ? -1: 1;
        }
    }
}
