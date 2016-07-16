package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageFile;
import com.alibaba.middleware.race.table.Row;
import com.alibaba.middleware.race.utils.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class Writer implements Runnable {

    private LinkedBlockingQueue<Row> in;
    private Data buffer;
    private Row row;
    private PageFile pageFile;
    private long inCount;
    private long threadId;

    public Writer(LinkedBlockingQueue<Row> in, PageFile pageFile) {
        this.in = in;
        this.pageFile = pageFile;
        this.buffer = new Data(new byte[Constants.PAGE_SIZE*(1<<10)]);
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
        System.out.println("INFO: Writer thread is running. Thread id:" + threadId);
        while (true) {
            this.nextRow();
            if(row.isEmpty()) {
                break;
            }
            int len = row.writeToBytes(buffer);
            inCount++;
            if(inCount % 30 == 0) {
                System.out.println("INFO: Writer count is:" + inCount + ". Thread id:" + threadId);
            }
        }
        System.out.println("INFO: Writer thread completed. Thread id:" + threadId);
    }
}
