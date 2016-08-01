package com.alibaba.middleware.race.cache;


public class IndexEntry implements Comparable<IndexEntry> {
    public short prefix;
    private byte fileId;
    private int offset;
    private int length;
    public IndexEntry next;

    public IndexEntry(short prefix, byte fileId, int offset, int length) {
        this.prefix = prefix;
        this.fileId = fileId;
        this.offset = offset;
        this.length = length;
        this.next = null;
    }

    public byte getFileId() {
        return fileId;
    }

    public long getAddress() {
        return 0xffffffffL&offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public int compareTo(IndexEntry o) {
        if (fileId == o.fileId) {
            return offset < o.offset ? -1: 1;
        } else {
            return fileId < o.fileId ? -1: 1;
        }
    }
}
