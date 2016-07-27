package com.alibaba.middleware.race.index;

public class BuyerIdRowIndex extends RowIndex {

    private long createTime;

    public BuyerIdRowIndex(RecordIndex recodeIndex, int hashCode, long createTime) {
        super(recodeIndex, hashCode);
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return createTime;
    }
}
