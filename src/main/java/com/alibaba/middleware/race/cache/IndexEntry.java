package com.alibaba.middleware.race.cache;

public class IndexEntry {
    public short prefix;
    private byte fileId;
    private int offset;
    public IndexEntry next;

    public IndexEntry(short prefix, byte fileId, int offset) {
        this.prefix = prefix;
        this.fileId = fileId;
        this.offset = offset;
        this.next = null;
    }

    public byte getFileId() {
        return fileId;
    }

    public long getAddress() {
        return offset;
    }
}
