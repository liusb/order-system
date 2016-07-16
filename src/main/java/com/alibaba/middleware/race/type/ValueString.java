package com.alibaba.middleware.race.type;

public class ValueString extends Value {

    private final String value;

    public ValueString(String value) {
        this.value = value;
    }

    @Override
    public byte getType() {
        return Value.STRING;
    }

    @Override
    public int getMemory() {
        return value.length();
    }

    public String getValue() {
        return value;
    }
}
