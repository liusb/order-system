package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.*;
import com.alibaba.middleware.race.table.OffsetLine;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderParser implements Runnable {
    private LinkedBlockingQueue<OffsetLine> in;
    private OffsetLine line;
    private ArrayList<LinkedBlockingQueue<GoodIdRowIndex>> goodIndexOuts;
    private ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexOuts;
    private ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexOuts;
    private HashIndex goodIndexIndex;
    private HashIndex orderIndexIndex;
    private HashIndex buyerIndexIndex;
    private int rowCount;
    private long threadId;

    public OrderParser(LinkedBlockingQueue<OffsetLine> inQueue,
                       ArrayList<LinkedBlockingQueue<GoodIdRowIndex>> goodOIndexQueues,
                       ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexQueues,
                       ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexQueues,
                       HashIndex goodIndexIndex, HashIndex orderIndexIndex,
                       HashIndex buyerIndexIndex) {
        this.in = inQueue;
        this.goodIndexOuts = goodOIndexQueues;
        this.orderIndexOuts = orderIndexQueues;
        this.buyerIndexOuts = buyerIndexQueues;
        this.goodIndexIndex = goodIndexIndex;
        this.orderIndexIndex = orderIndexIndex;
        this.buyerIndexIndex = buyerIndexIndex;
        this.line = null;
        this.rowCount = 0;
        this.threadId = 0;
    }

    private void nextLine() {
        while (true) {
            line = null;
            try {
                line = in.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (line != null) {
                return;
            }
        }
    }

    @Override
    public void run() {
        this.threadId = Thread.currentThread().getId();
        while (true) {
            this.nextLine();
            if(line.getLine().isEmpty()) {
                break;
            }
            StringTokenizer tokenizer = new StringTokenizer(line.getLine(), ":\t");
            String key;
            String value;
            long orderId=0, createTime=0;
            String goodId="", buyerId = "";
            short findCount = 0;
            while (tokenizer.hasMoreTokens()) {
                key = tokenizer.nextToken();
                value = tokenizer.nextToken();
                if (key.equals("orderid")) {
                    findCount++;
                    orderId = Long.parseLong(value);
                } else if (key.equals("createtime")) {
                    findCount++;
                    createTime = Long.parseLong(value);
                } else if (key.equals("buyerid")){
                    findCount++;
                    buyerId = value;
                } else if (key.equals("goodid")) {
                    findCount++;
                    goodId = value;
                }
                if (findCount == 4) {
                    break;
                }
            }
            int goodHash = HashIndex.getHashCode(goodId);
            int orderHash = HashIndex.getHashCode(orderId);
            int buyerHash = HashIndex.getHashCode(buyerId);
            GoodIdRowIndex goodRowIndex = new GoodIdRowIndex(line.getRecodeIndex(), goodHash, goodId);
            OrderIdRowIndex orderRowIndex = new OrderIdRowIndex(line.getRecodeIndex(), orderHash, orderId);
            BuyerIdRowIndex buyerIdRowIndex =  new BuyerIdRowIndex(line.getRecodeIndex(), buyerHash, buyerId, createTime);
            while (true) {
                try {
                    goodIndexOuts.get(goodIndexIndex.getFileIndex(goodHash)).put(goodRowIndex);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            while (true) {
                try {
                    orderIndexOuts.get(orderIndexIndex.getFileIndex(orderHash)).put(orderRowIndex);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            while (true) {
                try {
                    buyerIndexOuts.get(buyerIndexIndex.getFileIndex(buyerHash)).put(buyerIdRowIndex);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            rowCount ++;
        }
        System.out.println("INFO: Parser thread completed. rowCount:" + rowCount + " Thread id:" + threadId);
    }
}
