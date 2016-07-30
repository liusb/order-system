package com.alibaba.middleware.race.index;

public class RecordIndex implements Comparable<RecordIndex> {
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

    @Override
    public int compareTo(RecordIndex o) {
        if (fileId == o.fileId) {
            return address < o.address ? -1: 1;
        } else {
            return fileId < o.fileId ? -1: 1;
        }
    }
}
