package com.alibaba.middleware.race.query;

public class BuyerCondition {
    public long postfix;
    public long startTime;
    public long endTime;

    public BuyerCondition(long postfix, long startTime, long endTime) {
        this.postfix = postfix;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
