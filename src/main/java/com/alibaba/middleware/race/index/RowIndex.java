package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.store.Data;

public abstract class RowIndex {
    // todo 修改为具体类，使查询返回的结果为这个类的实例，以减少内存使用

    public static final long EMPTY_FLAG = -1;

    private byte fileId;
    private long address;
    private int hashCode;

    public RowIndex(byte fileId, long address) {
        this.fileId = fileId;
        this.address = address;
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

    public boolean isEmpty() {
        return address == EMPTY_FLAG;
    }

    public abstract void writeToBuffer(Data buffer);

}
