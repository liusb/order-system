package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.store.Data;

public class BuyerIdRowIndex extends RowIndex {

    private String buyerId;
    private long createTime;

    public BuyerIdRowIndex(byte fileId, long address) {
        super(fileId, address);
    }

    @Override
    public void writeToBuffer(Data buffer) {
        buffer.reset();
        // [hashCode(int), buyerId(len,string), createTime(long), fileId(byte), address(long)]
        buffer.writeInt(this.getHashCode());
        buffer.writeString(this.buyerId);
        buffer.writeLong(this.createTime);
        buffer.writeByte(this.getFileId());
        buffer.writeLong(this.getAddress());
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public String getBuyerId() {
        return this.buyerId;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return this.createTime;
    }
}
