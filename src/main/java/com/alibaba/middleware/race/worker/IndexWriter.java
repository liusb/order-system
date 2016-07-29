package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.*;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;

import java.util.concurrent.LinkedBlockingQueue;

public class IndexWriter<T extends RowIndex> implements Runnable {

    private LinkedBlockingQueue<T> in;
    private T row;
    private Data buffer;
    private PageStore pageFile;
    private HashIndex index;

    public IndexWriter(LinkedBlockingQueue<T> in,
                       PageStore pageFile, HashIndex index) {
        this.in = in;
        this.row = null;
        this.pageFile = pageFile;
        this.index = index;
        this.buffer = new Data(new byte[256]);
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

    private void writeToBuffer(RecordIndex recordIndex) {
        buffer.writeByte(recordIndex.getFileId());
        buffer.writeLong(recordIndex.getAddress());
    }

    private void writeToBuffer(GoodIdRowIndex goodIdRowIndex) {
        buffer.writeInt(goodIdRowIndex.getHashCode());
        buffer.writeString(goodIdRowIndex.getGoodId());
        writeToBuffer(goodIdRowIndex.getRecodeIndex());
    }

    private void writeToBuffer(OrderIdRowIndex orderIdRowIndex) {
        buffer.writeLong(orderIdRowIndex.getOrderId());
        writeToBuffer(orderIdRowIndex.getRecodeIndex());
    }

    private void writeToBuffer(BuyerIdRowIndex buyerIdRowIndex) {
        buffer.writeInt(buyerIdRowIndex.getHashCode());
        buffer.writeString(buyerIdRowIndex.getBuyerId());
        buffer.writeLong(buyerIdRowIndex.getCreateTime());
        writeToBuffer(buyerIdRowIndex.getRecodeIndex());
    }


    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();
        long beginMillis = System.currentTimeMillis();
        int inCount = 0;
        while (true) {
            this.nextRow();
            if(row.getRecodeIndex().getAddress()==-1) {
                break;
            }
            buffer.reset();
            if (row instanceof GoodIdRowIndex) {
                writeToBuffer((GoodIdRowIndex)row);
            } else if (row instanceof OrderIdRowIndex) {
                writeToBuffer((OrderIdRowIndex) row);
            } else {
                writeToBuffer((BuyerIdRowIndex) row);
            }
            int bucketId = index.getBucketId(row.getHashCode());
            pageFile.insertIndexData(bucketId, buffer);
            inCount++;
            if((inCount & ((1<<24)-1)) == 0) {
                System.out.println("INFO: Write " + inCount + " Order used "
                        + (System.currentTimeMillis() - beginMillis) + "millis in thread " + threadId);
            }
        }
        this.pageFile.close();
        System.out.println("INFO: Writer thread completed. inCount:" + inCount + " Thread id:" + threadId);
    }
}
