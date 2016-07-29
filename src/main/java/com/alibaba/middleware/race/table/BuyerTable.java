package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.TwoLevelCache;

import java.util.Collection;
import java.util.HashMap;

public class BuyerTable {
    private static BuyerTable instance = new BuyerTable();
    public static BuyerTable getInstance() {
        return instance;
    }
    private BuyerTable() { }

    private TwoLevelCache<String, HashMap<String, Object>> resultCache;

//    private static final int TABLE_BUCKET_SIZE = 256;

    private static final int TABLE_BUCKET_SIZE = 256*(1<<10);

    // 每页的大小，单位为byte
    private static final int TABLE_PAGE_SIZE = 4*(1<<10);
    private static final String[] TABLE_COLUMNS = {"buyerid"};
    public HashTable baseTable;


    // 在构造之前做初始工作
    public void init(Collection<String> storeFolders) {
        baseTable = new HashTable("buyerTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        baseTable.init(storeFolders, TABLE_BUCKET_SIZE, TABLE_PAGE_SIZE);
    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {
        this.baseTable.reopen();
        resultCache = new TwoLevelCache<String, HashMap<String, Object>>(512*1024, 128*1024);
    }


    public HashMap<String, Object> find(String buyerId) {
        HashMap<String, Object> result = resultCache.get(buyerId);
        if (result == null) {
            result = this.baseTable.findRecord(buyerId);
            if (result != null) {
                resultCache.put(buyerId, result);
            }
        } else {
//            System.out.println("命中缓存 buyerId: " + buyerId);
        }
        return result;
    }
}
