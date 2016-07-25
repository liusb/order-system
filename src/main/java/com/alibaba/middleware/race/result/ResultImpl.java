package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.utils.MathUtils;

import java.util.HashMap;

public class ResultImpl implements OrderSystem.Result, Comparable<ResultImpl> {

    private long orderId;
    private HashMap<String, KVImpl> keyValue;
    private long createTime;

    public ResultImpl(long orderId, HashMap<String, KVImpl> keyValue) {
        this.orderId = orderId;
        this.keyValue = keyValue;
        this.createTime = -1;
    }

    public ResultImpl(long orderId, HashMap<String, KVImpl> keyValue, long createTime) {
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{orderid:" + orderId + ", KV:[");
        if (keyValue != null && !keyValue.isEmpty()) {
            for (KVImpl field : keyValue.values()) {
                sb.append(field.toString());
                sb.append(",");
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
