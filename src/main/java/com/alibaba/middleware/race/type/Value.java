package com.alibaba.middleware.race.type;

public abstract class Value {

    public static final byte BOOLEAN_FALSE = 0;
    public static final byte BOOLEAN_TRUE = 1;
    public static final byte LONG = 2;
    public static final byte DOUBLE = 3;
    public static final byte STRING = 4;

    public static final byte INT = 5;
    public static final byte BYTE = 6;
    public static final byte BYTES = 7;


    public abstract byte getType();
    public abstract int getMemory();

}
