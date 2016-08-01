package com.alibaba.middleware.race.query;

import com.alibaba.middleware.race.cache.IndexEntry;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class IndexAttachment {

    public HashMap<String, String> record;
    public byte[] buffer;
    public CountDownLatch latch;
    public IndexEntry indexEntry;

    public IndexAttachment(CountDownLatch latch, IndexEntry indexEntry) {
        this.latch = latch;
        this.indexEntry = indexEntry;
        this.record = new HashMap<String, String>();
        this.buffer = new byte[indexEntry.getLength()];
    }

    public IndexAttachment(CountDownLatch latch, HashMap<String, String> record, IndexEntry indexEntry) {
        this.latch = latch;
        this.indexEntry = indexEntry;
        this.record = record;
        this.buffer = new byte[indexEntry.getLength()];
    }

}
