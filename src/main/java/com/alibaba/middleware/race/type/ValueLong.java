package com.alibaba.middleware.race.type;

public class ValueLong extends Value {
    private final long value;

    public ValueLong(long value) {
        this.value = value;
    }

    @Override
    public byte getType() {
        return Value.LONG;
    }

    @Override
    public int getMemory() {
        return 8;
    }

    public long getValue() {
        return value;
    }
}
