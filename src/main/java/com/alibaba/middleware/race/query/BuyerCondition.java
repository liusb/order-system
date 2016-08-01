package com.alibaba.middleware.race.query;

public class BuyerCondition {
    public short prefix;
    public long postfix;
    public long startTime;
    public long endTime;

    public BuyerCondition(short prefix, long postfix, long startTime, long endTime) {
        this.prefix = prefix;
        this.postfix = postfix;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
