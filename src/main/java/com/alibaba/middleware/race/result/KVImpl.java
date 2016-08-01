package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

public class KVImpl implements OrderSystem.KeyValue {

    private String key;
    private String value;

    public KVImpl(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public String valueAsString() {
        return value;
    }

    @Override
    public long valueAsLong() throws OrderSystem.TypeException {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        if (value.equals("false")) {
            return false;
        } else if (value.equals("true")) {
            return true;
        } else {
            throw new OrderSystem.TypeException();
        }
    }

    @Override
    public String toString() {
        if (key.equals("contactphone")) {
            try {
                Long longValue = Long.parseLong(this.value);
                return  this.key + ":" + longValue.toString();
            } catch (Exception e) {
            }
        }
        return  this.key + ":" + this.value;
    }

    public static void main(String[] args) {
        System.out.println(new KVImpl("contactphone", "0523731460724"));
    }
}
