package com.alibaba.middleware.race.index;

public class BuyerIdRowIndex extends RowIndex {

    private String buyerId;
    private long createTime;

    public BuyerIdRowIndex(RecordIndex recodeIndex, int hashCode, String buyerId, long createTime) {
        super(recodeIndex, hashCode);
        this.buyerId = buyerId;
        this.createTime = createTime;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public long getCreateTime() {
        return createTime;
    }
}
