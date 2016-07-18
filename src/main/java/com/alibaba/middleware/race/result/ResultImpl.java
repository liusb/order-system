package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

import java.util.HashMap;

public class ResultImpl implements OrderSystem.Result {
    private long orderId;
    private HashMap<String, OrderSystem.KeyValue> keyValue;

    private ResultImpl(long orderId, HashMap<String, OrderSystem.KeyValue> keyValue) {
        this.orderId = orderId;
        this.keyValue = keyValue;
    }
    @Override
    public OrderSystem.KeyValue get(String key) {
        return this.keyValue.get(key);
    }

    @Override
    public OrderSystem.KeyValue[] getAll() {
        return this.keyValue.values().toArray(new OrderSystem.KeyValue[0]);
    }

    @Override
    public long orderId() {
        return this.orderId;
    }
}
