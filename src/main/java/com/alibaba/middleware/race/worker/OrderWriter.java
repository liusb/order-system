package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.Row;
import com.alibaba.middleware.race.utils.Constants;

import java.util.ArrayList;
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
    private ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexOut;
    private int buyerIdColumnId;
    private int buyerCreateTimeColumnId;
    private HashIndex buyerIndexIndex;
    private ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexOut;

    private long inCount;
    private long threadId;

    public OrderWriter(LinkedBlockingQueue<Row> in, PageStore pageFile, byte fileId, HashIndex index,
                       HashIndex orderIndexIndex, ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexOut,
                       HashIndex buyerIndexIndex, ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexOut,
                       int orderColumnId, int buyerIdColumnId, int buyerCreateTimeColumnId) {
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
        this.orderColumnId = orderColumnId;
        this.buyerIdColumnId = buyerIdColumnId;
        this.buyerCreateTimeColumnId = buyerCreateTimeColumnId;
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

    private static <T> void sendToQueue(LinkedBlockingQueue<T> queue, T row) {
        while (true) {
            try {
                queue.put(row);
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void outputOrderIndex() {
        OrderIdRowIndex orderIdRowIndex = new OrderIdRowIndex(this.fileId, this.address);
        long orderId = ((Long) row.getValue(orderColumnId));
        int hashCode = HashIndex.getHashCode(row.getValue(orderColumnId));
        orderIdRowIndex.setOrderId(orderId);
        sendToQueue(orderIndexOut.get(orderIndexIndex.getFileIndex(hashCode)), orderIdRowIndex);
    }

    private void outputBuyerIndex() {
        BuyerIdRowIndex buyerIdRowIndex = new BuyerIdRowIndex(this.fileId, this.address);
        String buyerId = ((String) row.getValue(buyerIdColumnId));
        long createTime = ((Long) row.getValue(buyerCreateTimeColumnId));
        int hashCode = HashIndex.getHashCode(buyerId);
        buyerIdRowIndex.setBuyerId(buyerId);
        buyerIdRowIndex.setHashCode(hashCode);
        buyerIdRowIndex.setCreateTime(createTime);
        sendToQueue(buyerIndexOut.get(buyerIndexIndex.getFileIndex(hashCode)), buyerIdRowIndex);
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
            int PageId = index.getBucketIndex(row.getHashCode());
            this.address = pageFile.insertData(PageId, buffer);
            // 输出索引
            this.outputOrderIndex();
            this.outputBuyerIndex();
            inCount++;
//            if(inCount % 30 == 0) {
//                System.out.println("INFO: Writer count is:" + inCount + ". Thread id:" + threadId);
//            }
        }
        this.pageFile.close();
        System.out.println("INFO: Writer thread completed. inCount:" + inCount + " Thread id:" + threadId);
    }
}
