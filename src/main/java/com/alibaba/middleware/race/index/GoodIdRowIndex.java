package com.alibaba.middleware.race.index;

public class GoodIdRowIndex extends RowIndex {

    private String goodId;

    public GoodIdRowIndex(RecordIndex recodeIndex, int hashCode, String goodId) {
        super(recodeIndex, hashCode);
        this.goodId = goodId;
    }

    public String getGoodId() {
        return goodId;
    }
}
