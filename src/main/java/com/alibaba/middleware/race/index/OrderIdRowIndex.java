package com.alibaba.middleware.race.index;

public class OrderIdRowIndex extends RowIndex {

    private long orderId;

    public OrderIdRowIndex(RecordIndex recodeIndex, int hashCode, long orderId) {
        super(recodeIndex, hashCode);
        this.orderId = orderId;
    }

    public long getOrderId() {
        return orderId;
    }
}
