package com.alibaba.middleware.race.utils;

public class MathUtils {

    public static int compareInt(int a, int b) {
        return a == b ? 0 : a < b ? -1 : 1;
    }

}
