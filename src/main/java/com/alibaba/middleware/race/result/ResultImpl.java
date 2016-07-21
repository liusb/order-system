package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.utils.MathUtils;

import java.util.HashMap;

public class ResultImpl implements OrderSystem.Result, Comparable<ResultImpl> {

    private long orderId;
    private HashMap<String, OrderSystem.KeyValue> keyValue;
    private long createTime;

    public ResultImpl(long orderId, HashMap<String, OrderSystem.KeyValue> keyValue) {
        this.orderId = orderId;
        this.keyValue = keyValue;
        this.createTime = -1;
    }

    public ResultImpl(long orderId, HashMap<String, OrderSystem.KeyValue> keyValue, long createTime) {
        this.orderId = orderId;
        this.keyValue = keyValue;
        this.createTime = createTime;
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

    @Override
    public int compareTo(ResultImpl o) {
        if (createTime == -1) {
            return MathUtils.compareLong(o.orderId, this.orderId);
        } else {
            return MathUtils.compareLong(this.createTime, o.createTime);
        }
    }
}
