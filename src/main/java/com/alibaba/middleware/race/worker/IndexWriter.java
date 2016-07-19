package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.utils.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class IndexWriter implements Runnable {

    private LinkedBlockingQueue<RowIndex> in;
    private Data buffer;
    private RowIndex row;
    private PageStore pageFile;
    private HashIndex index;
    private long inCount;
    private long threadId;

    public IndexWriter(LinkedBlockingQueue<RowIndex> in,
                       PageStore pageFile, HashIndex index) {
        this.in = in;
        this.pageFile = pageFile;
        this.index = index;
        this.buffer = new Data(new byte[Constants.PAGE_SIZE]);
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

    private void writeToBuffer() {
        buffer.reset();
        Object[] values = row.getValues();
        if (values.length == 1) {
            buffer.writeLong(((Long)values[0]));
        } else {
            buffer.writeInt(row.getHashCode());
            for (Object value : row.getValues()) {
                if (value instanceof Long) {
                    buffer.writeLong(((Long) value));
                } else if (value instanceof String) {
                    buffer.writeString(((String) value));
                } else {
                    throw new RuntimeException("暂时不支持其他类型");
                }
            }
        }
        buffer.writeByte(row.getFileId());
        buffer.writeLong(row.getAddress());
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
            writeToBuffer();
            int PageId = index.getBucketIndex(row.getHashCode());
            pageFile.insertData(PageId, buffer);
            inCount++;
            if(inCount % 30 == 0) {
                System.out.println("INFO: Writer count is:" + inCount + ". Thread id:" + threadId);
            }
        }
        this.pageFile.close();
        System.out.println("INFO: Writer thread completed. Thread id:" + threadId);
    }
}
