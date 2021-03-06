package com.alibaba.middleware.race.query;

import com.alibaba.middleware.race.index.RecordIndex;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class RecordAttachment {

    public HashMap<String, String> record;
    public byte[] buffer;
    public CountDownLatch latch;
    public RecordIndex recordIndex;

    public RecordAttachment(CountDownLatch latch, RecordIndex recordIndex, int bufferSize) {
        this.latch = latch;
        this.recordIndex = recordIndex;
        this.record = new HashMap<String, String>();
        this.buffer = new byte[bufferSize];
    }
}
