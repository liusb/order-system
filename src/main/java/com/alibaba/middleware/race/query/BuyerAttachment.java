package com.alibaba.middleware.race.query;

import com.alibaba.middleware.race.result.ResultImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class BuyerAttachment {

    public byte[] buffer;
    public BuyerCondition condition;
    public CountDownLatch waitBuyerLatch;
    public HashMap<String, String> buyerRecord;
    public CountDownLatch waitForResult;
    public ArrayList<ResultImpl> resultsSet;

    public BuyerAttachment(BuyerCondition condition, byte[] buffer,
                           CountDownLatch waitBuyerLatch, HashMap<String, String> buyerRecord,
                           CountDownLatch waitForResult, ArrayList<ResultImpl> resultsSet) {
        this.condition = condition;
        this.buffer = buffer;
        this.waitBuyerLatch = waitBuyerLatch;
        this.buyerRecord = buyerRecord;
        this.waitForResult = waitForResult;
        this.resultsSet = resultsSet;
    }
}
