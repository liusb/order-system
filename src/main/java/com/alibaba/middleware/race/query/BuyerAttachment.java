package com.alibaba.middleware.race.query;

import com.alibaba.middleware.race.OrderSystem;

import java.nio.channels.AsynchronousFileChannel;
import java.util.HashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

public class BuyerAttachment {

    public byte[] buffer;
    public AsynchronousFileChannel fileChannel;
    public BuyerCondition condition;
    public CountDownLatch waitForResultLatch;
    public CountDownLatch waitBuyerLatch;
    public HashMap<String, String> buyerRecord;
    public ConcurrentSkipListSet<OrderSystem.Result> resultsSet;

    public BuyerAttachment(BuyerCondition condition, AsynchronousFileChannel fileChannel, int bufferSize,
                           CountDownLatch waitBuyerLatch, HashMap<String, String> buyerRecord,
                           CountDownLatch waitForResultLatch, ConcurrentSkipListSet<OrderSystem.Result> resultsSet) {
        this.condition = condition;
        this.fileChannel = fileChannel;
        this.buffer = new byte[bufferSize];
        this.waitForResultLatch = waitForResultLatch;
        this.waitBuyerLatch = waitBuyerLatch;
        this.buyerRecord = buyerRecord;
        this.resultsSet = resultsSet;
    }
}
