package com.alibaba.middleware.race.index;

public class RecordIndex {
    private final byte fileId;
    private final long address;

    public RecordIndex(byte fileId, long address) {
        this.fileId = fileId;
        this.address = address;
    }

    public byte getFileId() {
        return fileId;
    }

    public long getAddress() {
        return address;
    }
}
