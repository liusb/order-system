package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.Row;
import com.alibaba.middleware.race.utils.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class OrderWriter implements Runnable {

    private LinkedBlockingQueue<Row> in;
    private Data buffer;
    private Row row;
    private PageStore pageFile;
    private byte fileId;
    private long address;
    private HashIndex index;

    private int orderColumnId;
    private HashIndex orderIndexIndex;
    private LinkedBlockingQueue<RowIndex> orderIndexOut;
    private int buyerIdColumnId;
    private int buyerCreateTimeColumnId;
    private HashIndex buyerIndexIndex;
    private LinkedBlockingQueue<RowIndex> buyerIndexOut;

    private long inCount;
    private long threadId;

    public OrderWriter(LinkedBlockingQueue<Row> in, PageStore pageFile, byte fileId, HashIndex index,
                       HashIndex orderIndexIndex, LinkedBlockingQueue<RowIndex> orderIndexOut,
                       HashIndex buyerIndexIndex, LinkedBlockingQueue<RowIndex> buyerIndexOut) {
        this.in = in;
        this.pageFile = pageFile;
        this.index = index;
        this.fileId = fileId;
        this.address = 0;
        this.orderIndexIndex = orderIndexIndex;
        this.orderIndexOut = orderIndexOut;
        this.buyerIndexIndex = buyerIndexIndex;
        this.buyerIndexOut = buyerIndexOut;
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

    private void outputOrderIndex(long address) {
        RowIndex rowIndex = new RowIndex(this.fileId, this.address, 1);
        int hashCode = HashIndex.getHashCode(row.getValue(orderColumnId));
    }

    private void outputBuyerIndex() {

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
            row.writeToBytes(buffer);
            int PageId = index.getBucketIndex(row.getHashCode());
            long address = pageFile.insertData(PageId, buffer);
            // 输出索引
            this.outputOrderIndex(address);
            this.outputBuyerIndex();
            inCount++;
            if(inCount % 30 == 0) {
                System.out.println("INFO: Writer count is:" + inCount + ". Thread id:" + threadId);
            }
        }
        this.pageFile.close();
        System.out.println("INFO: Writer thread completed. Thread id:" + threadId);
    }
}
