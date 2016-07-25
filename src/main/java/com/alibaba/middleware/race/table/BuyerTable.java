package com.alibaba.middleware.race.table;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;

public class BuyerTable {
    private static BuyerTable instance = new BuyerTable();
    public static BuyerTable getInstance() {
        return instance;
    }
    private BuyerTable() { }


    // 哈希桶的总个数
    private static final int TABLE_BUCKET_SIZE = 256*(1<<10);
    // 建立表示很LRU缓存的大小
    private static final int TABLE_CACHE_SIZE = 256*(1<<10);
    // 每页的大小，单位为byte
    private static final int TABLE_PAGE_SIZE = 4*(1<<10);
    private static final String[] TABLE_COLUMNS = {"buyerid"};
    public HashTable baseTable;


    // 在构造之前做初始工作
    public void init(Collection<String> storeFolders) {
        baseTable = new HashTable("buyerTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        baseTable.init(storeFolders, TABLE_BUCKET_SIZE, TABLE_CACHE_SIZE, TABLE_PAGE_SIZE);
    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {
        this.baseTable.reopen(4*(1<<10));
    }

    public HashMap<String, Object> find(String buyerId, TreeMap<Integer, String> keys) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        return result;
    }

    public HashMap<String, Object> find(String buyerId) {
        return this.baseTable.findRecord(buyerId);
    }
}
