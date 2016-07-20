package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OrderTable {
    private static OrderTable instance = new OrderTable();
    public static OrderTable getInstance() {
        return instance;
    }
    private OrderTable() {}


    // 哈希桶的总个数
    private static final int TABLE_BUCKET_SIZE = 1024;
    // 建立表示很LRU缓存的大小
    private static final int TABLE_CACHE_SIZE = 1024;
    // 每页的大小，单位为byte
    private static final int TABLE_PAGE_SIZE = 16*(1<<10);
    // 本身存储 索引为goodId, 存储格式为[goodId, orderId, buyerId, createTime, ......]
    private static final String[] TABLE_COLUMNS = {"goodid", "orderid", "buyerid", "createtime"};
    public HashTable baseTable;

    private static final int ORDER_INDEX_BUCKET_SIZE = 1024;
    private static final int ORDER_INDEX_CACHE_SIZE = 1024;
    private static final int ORDER_INDEX_PAGE_SIZE = 16*(1<<10);
    // 索引为orderId 记录存储格式为[orderId, fileId, address](long, byte, long)
    public HashTable orderIndex;


    private static final int BUYER_INDEX_BUCKET_SIZE = 1024;
    private static final int BUYER_INDEX_CACHE_SIZE = 1024;
    private static final int BUYER_INDEX_PAGE_SIZE = 16*(1<<10);
    // 索引为buyerId, 记录存储格式为[createTime, buyerId, fileId, address](long, string, byte, long)
    public HashTable buyerCreateTimeIndex;

    public void init(Collection<String> storeFolders) {
        baseTable = new HashTable("orderTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        baseTable.init(storeFolders, TABLE_BUCKET_SIZE, TABLE_CACHE_SIZE, TABLE_PAGE_SIZE);

        orderIndex = new HashTable("orderIndex");
//        HashMap<String, Column> orderIndexColumns = new HashMap<String, Column>();
//        orderIndexColumns.put("orderid", baseTable.getColumn("orderid"));
//        orderIndex.setBaseColumns(orderIndexColumns);
        orderIndex.init(storeFolders, ORDER_INDEX_BUCKET_SIZE, ORDER_INDEX_CACHE_SIZE, ORDER_INDEX_PAGE_SIZE);

        buyerCreateTimeIndex = new HashTable("buyerCreateTimeIndex");
//        HashMap<String, Column> buyerIndexColumns = new HashMap<String, Column>();
//        orderIndexColumns.put("buyerid", baseTable.getColumn("buyerid"));
//        orderIndexColumns.put("createtime", baseTable.getColumn("createtime"));
//        buyerCreateTimeIndex.setBaseColumns(buyerIndexColumns);
        buyerCreateTimeIndex.init(storeFolders, BUYER_INDEX_BUCKET_SIZE, BUYER_INDEX_CACHE_SIZE, BUYER_INDEX_PAGE_SIZE);
    }

    public void reopen() {

    }

    public OrderIdRowIndex findOderIdIndex(long orderId) {
        return this.orderIndex.findIndex(orderId);
    }

    public HashMap<String, Object> findOrders(OrderIdRowIndex orderIdRowIndex) {
        return null;
    }

    public ArrayList<BuyerIdRowIndex> findBuyerIdIndex(String buyerId, long startTime, long endTime) {
        return this.buyerCreateTimeIndex.findIndex(buyerId, startTime, endTime);
    }

    public ArrayList<HashMap<String, Object>> findOrders(ArrayList<BuyerIdRowIndex> buyerIdRowIndices) {
        return null;
    }

    public ArrayList<HashMap<String, Object>> findOrders(String goodId) {
        return null;
    }
}
