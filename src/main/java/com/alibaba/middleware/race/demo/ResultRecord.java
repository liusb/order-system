package com.alibaba.middleware.race.demo;

import com.alibaba.middleware.race.OrderSystem;

import java.util.Set;

public class ResultRecord implements OrderSystem.Result {
    private long orderId;
    private Record kvMap;

    private ResultRecord(long orderId, Record kv) {
        this.orderId = orderId;
        this.kvMap = kv;
    }

    static public ResultRecord createResultRow(Record orderData, Record buyerData,
                                                Record goodData, Set<String> queryingKeys) {
        if (orderData == null || buyerData == null || goodData == null) {
            throw new RuntimeException("Bad data!");
        }
        Record allkv = new Record();
        long orderid;
        try {
            orderid = orderData.get("orderid").valueAsLong();
        } catch (OrderSystem.TypeException e) {
            throw new RuntimeException("Bad data!");
        }

        for (Field field : orderData.values()) {
            if (queryingKeys == null || queryingKeys.contains(field.key)) {
                allkv.put(field.key(), field);
            }
        }
        for (Field field : buyerData.values()) {
            if (queryingKeys == null || queryingKeys.contains(field.key)) {
                allkv.put(field.key(), field);
            }
        }
        for (Field field : goodData.values()) {
            if (queryingKeys == null || queryingKeys.contains(field.key)) {
                allkv.put(field.key(), field);
            }
        }
        return new ResultRecord(orderid, allkv);
    }

    public OrderSystem.KeyValue get(String key) {
        return this.kvMap.get(key);
    }

    public OrderSystem.KeyValue[] getAll() {
        return kvMap.values().toArray(new OrderSystem.KeyValue[0]);
    }

    public long orderId() {
        return orderId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("orderid: " + orderId + " {");
        if (kvMap != null && !kvMap.isEmpty()) {
            for (Field field : kvMap.values()) {
                sb.append(field.toString());
                sb.append(",\n");
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
