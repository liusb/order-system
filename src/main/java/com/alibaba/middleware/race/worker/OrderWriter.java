package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.Row;

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
    private long beginMillis;

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
        this.buffer = new Data(new byte[128*1024]);
        this.row = null;
        this.orderColumnId = orderColumnId;
        this.buyerIdColumnId = buyerIdColumnId;
        this.buyerCreateTimeColumnId = buyerCreateTimeColumnId;
        this.inCount = 0;
        this.threadId = 0;
        this.beginMillis = System.currentTimeMillis();
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
        orderIdRowIndex.setHashCode(hashCode);
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
        while (true) {
            this.nextRow();
            if(row.isEmpty()) {
                break;
            }
            row.writeToBytes(buffer);
            int bucketId = index.getBucketId(row.getHashCode());
            this.address = pageFile.insertData(bucketId, buffer);
            // 输出索引
            this.outputOrderIndex();
            this.outputBuyerIndex();
            inCount++;
            if((inCount & ((1<<24)-1)) == 0) {
                System.out.println("INFO: Write " + inCount + "Order used "
                        + (System.currentTimeMillis()- beginMillis) + "millis in thread " + threadId);
            }
        }
        this.pageFile.close();
        System.out.println("INFO: OrderWriter thread completed. inCount:" + inCount + " Thread id:" + threadId);
    }
}
