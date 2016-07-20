package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.store.Data;

public class OrderIdRowIndex extends RowIndex {

    private long orderId;

    public OrderIdRowIndex(byte fileId, long address) {
        super(fileId, address);
    }

    @Override
    public void writeToBuffer(Data buffer) {
        buffer.reset();
        buffer.writeLong(this.orderId);
        buffer.writeByte(this.getFileId());
        buffer.writeLong(this.getAddress());
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public long getOrderId() {
        return orderId;
    }
}
