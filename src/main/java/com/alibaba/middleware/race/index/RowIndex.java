package com.alibaba.middleware.race.index;

public class RowIndex {

    private byte fileId;
    private long address;
    public Object[] values;
    private int hashCode;

    public RowIndex(byte fileId, long address, int len) {
        this.fileId = fileId;
        this.address = address;
        this.values = new Object[len];
    }

    public byte getFileId() {
        return fileId;
    }

    public long getAddress() {
        return address;
    }

    public void setHashCode(int hashCode) {
        this.hashCode = hashCode;
    }

    public int getHashCode() {
        return this.hashCode;
    }

    public Object[] getValues() {
        return this.values;
    }

    public boolean isEmpty() {
        return values.length == 0;
    }
}
