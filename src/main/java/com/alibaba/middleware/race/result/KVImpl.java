package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

public class KVImpl implements OrderSystem.KeyValue {

    private String key;
    private Object value;

    @Override
    public String key() {
        return key;
    }

    @Override
    public String valueAsString() {
        return value.toString();
    }

    @Override
    public long valueAsLong() throws OrderSystem.TypeException {
        if (value instanceof Long) {
            return ((Long) value);
        } else {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        if (value instanceof Double) {
            return ((Double) value);
        } else {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        if (value instanceof Boolean) {
            return ((Boolean) value);
        } else {
            throw new OrderSystem.TypeException();
        }
    }
}
