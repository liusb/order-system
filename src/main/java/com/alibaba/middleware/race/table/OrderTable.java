package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.RowIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class OrderTable {
    private static OrderTable instance = new OrderTable();
    public static OrderTable getInstance() {
        return instance;
    }
    private OrderTable() {}


//    private static final int TABLE_BUCKET_SIZE = 128;
//    private static final int TABLE_CACHE_SIZE = 128;
//    private static final int REOPEN_TABLE_CACHE_SIZE = 128;
//    private static final int ORDER_INDEX_BUCKET_SIZE = 64;
//    private static final int ORDER_INDEX_CACHE_SIZE = 64;
//    private static final int REOPEN_ORDER_INDEX_CACHE_SIZE = 16;
//    private static final int BUYER_INDEX_BUCKET_SIZE = 64;
//    private static final int BUYER_INDEX_CACHE_SIZE = 64;
//    private static final int REOPEN_BUYER_INDEX_CACHE_SIZE = 16;

    private static final int GOOD_TABLE_BUCKET_SIZE = 64*(1<<10);
    private static final int GOOD_TABLE_CACHE_SIZE = 64*(1<<10);
    private static final int GOOD_REOPEN_TABLE_CACHE_SIZE = 64*(1<<10);
    private static final int ORDER_INDEX_BUCKET_SIZE = 64*(1<<10);
    private static final int ORDER_INDEX_CACHE_SIZE = 64*(1<<10);
    private static final int REOPEN_ORDER_INDEX_CACHE_SIZE = 16*(1<<10);
    private static final int BUYER_INDEX_BUCKET_SIZE = 64*(1<<10);
    private static final int BUYER_INDEX_CACHE_SIZE = 64*(1<<10);
    private static final int REOPEN_BUYER_INDEX_CACHE_SIZE = 16*(1<<10);

    // 每页的大小，单位为byte
    private static final int GOOD_TABLE_PAGE_SIZE = 2*(1<<10);
    // 存储索引为goodId, 存储格式为[goodId, orderId, buyerId, createTime, ......]
    public HashTable goodIndex;

    private static final String[] INDEX_COLUMNS = {};   // 索引不需要列信息
    private static final int ORDER_INDEX_PAGE_SIZE = 2*(1<<10);  // 1KB
    // 索引为orderId 记录存储格式为[orderId, fileId, address](long, byte, long)
    public HashTable orderIndex;

    private static final int BUYER_INDEX_PAGE_SIZE = 2*(1<<10);
    // 索引为buyerId, 记录存储格式为[createTime, buyerId, fileId, address](long, string, byte, long)
    public HashTable buyerCreateTimeIndex;

    public HashMap<String, Byte> orderFilesMap;

    public void init(Collection<String> storeFolders, Collection<String> orderFiles) {
        goodIndex = new HashTable("orderTable");
        goodIndex.setBaseColumns(INDEX_COLUMNS);
        goodIndex.init(storeFolders, GOOD_TABLE_BUCKET_SIZE, GOOD_TABLE_CACHE_SIZE, GOOD_TABLE_PAGE_SIZE);

        orderIndex = new HashTable("orderIndex");
        orderIndex.setBaseColumns(INDEX_COLUMNS);
        orderIndex.init(storeFolders, ORDER_INDEX_BUCKET_SIZE, ORDER_INDEX_CACHE_SIZE, ORDER_INDEX_PAGE_SIZE);

        buyerCreateTimeIndex = new HashTable("buyerCreateTimeIndex");
        buyerCreateTimeIndex.setBaseColumns(INDEX_COLUMNS);
        buyerCreateTimeIndex.init(storeFolders, BUYER_INDEX_BUCKET_SIZE, BUYER_INDEX_CACHE_SIZE, BUYER_INDEX_PAGE_SIZE);

        orderFilesMap = new HashMap<String, Byte>(43);
        byte fileId=1;
        for (String file: orderFiles) {
            orderFilesMap.put(file, fileId++);
        }
    }

    private volatile boolean prepared = false;

    public boolean isPrepared() {
        return prepared;
    }

    public void reopen() {
        goodIndex.reopen(GOOD_REOPEN_TABLE_CACHE_SIZE);
        orderIndex.reopen(REOPEN_ORDER_INDEX_CACHE_SIZE);
        buyerCreateTimeIndex.reopen(REOPEN_BUYER_INDEX_CACHE_SIZE);
        this.prepared = true;
    }

    public RowIndex findOderIdIndex(long orderId) {
        return this.orderIndex.findIndex(orderId);
    }

    public HashMap<String, Object> findOrders(RowIndex orderIdRowIndex) {
        return this.baseTable.findOrder(orderIdRowIndex);
    }

    public ArrayList<BuyerIdRowIndex> findBuyerIdIndex(String buyerId, long startTime, long endTime) {
        return this.buyerCreateTimeIndex.findIndex(buyerId, startTime, endTime);
    }

    public ArrayList<HashMap<String, Object>> findOrders(ArrayList<BuyerIdRowIndex> buyerIdRowIndices) {
        ArrayList<HashMap<String, Object>> results = new ArrayList<HashMap<String, Object>>();
        for (BuyerIdRowIndex index: buyerIdRowIndices) {
            results.add(baseTable.findOrder(index));
        }
        return results;
    }

    public ArrayList<HashMap<String, Object>> findOrders(String goodId) {
        return this.baseTable.findOrders(goodId);
    }
}
