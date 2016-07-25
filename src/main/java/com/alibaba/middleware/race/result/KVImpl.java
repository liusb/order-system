package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

public class KVImpl implements OrderSystem.KeyValue {

    private String key;
    private Object value;

    public KVImpl(String key, Object value) {
        this.key = key;
        this.value = value;
    }

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
        } else if (value instanceof String) {
            try {
                return Long.parseLong(((String) value));
            } catch (NumberFormatException e) {
                throw new OrderSystem.TypeException();
            }
        } else {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        if (value instanceof Double) {
            return ((Double) value);
        } else if (value instanceof String) {
            try {
                return Double.parseDouble(((String) value));
            } catch (NumberFormatException e) {
                throw new OrderSystem.TypeException();
            }
        }else {
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

    @Override
    public String toString() {
        return "[" + this.key + "]:" + this.value;
    }
}
