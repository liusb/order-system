package com.alibaba.middleware.race.type;

public class ValueBoolean extends Value {
    static final public String booleanTrueValue = "true";
    static final public String booleanFalseValue = "false";

    public static final ValueBoolean TRUE = new ValueBoolean(true);
    public static final ValueBoolean FALSE = new ValueBoolean(false);

    private final boolean value;

    private ValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public byte getType() {
        return Value.BOOLEAN_FALSE;
    }

    @Override
    public int getMemory() {
        return 1;
    }

    public boolean getValue() {
        return value;
    }

}
