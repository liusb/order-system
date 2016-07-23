package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.Row;

import java.util.concurrent.LinkedBlockingQueue;

public class Writer implements Runnable {

    private LinkedBlockingQueue<Row> in;
    private Data buffer;
    private Row row;
    private PageStore pageFile;
    private HashIndex index;
    private long inCount;
    private long threadId;

    public Writer(LinkedBlockingQueue<Row> in, PageStore pageFile, HashIndex index) {
        this.in = in;
        this.pageFile = pageFile;
        this.index = index;
        this.buffer = new Data(new byte[128*1024]);
        this.row = null;
        this.inCount = 0;
        this.threadId = 0;
    }

    private void nextRow() {
        while (true) {
            row = null;
            try {
                row = in.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (row != null) {
                return;
            }
        }
    }

    @Override
    public void run() {
        this.threadId = Thread.currentThread().getId();
//        System.out.println("INFO: Writer thread is running. Thread id:" + threadId);
        while (true) {
            this.nextRow();
            if(row.isEmpty()) {
                break;
            }
            row.writeToBytes(buffer);
            int bucketId = index.getBucketId(row.getHashCode());
            pageFile.insertData(bucketId, buffer);
            inCount++;
//            if(inCount % 30 == 0) {
//                System.out.println("INFO: Writer count is:" + inCount + ". Thread id:" + threadId);
//            }
        }
        this.pageFile.close();
        System.out.println("INFO: Writer thread completed. inCount:" + inCount + " Thread id:" + threadId);
    }
}
