package com.alibaba.middleware.race.type;

public class ValueDouble extends Value {

    private final double value;

    public ValueDouble(double value) {
        this.value = value;
    }

    @Override
    public byte getType() {
        return Value.DOUBLE;
    }

    @Override
    public int getMemory() {
        return 8;
    }

    public double getValue() {
        return value;
    }
}
