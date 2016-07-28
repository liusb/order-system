package com.alibaba.middleware.race.table;


import java.util.Collection;
import java.util.HashMap;

public class GoodTable {
    private static GoodTable instance = new GoodTable();
    public static GoodTable getInstance() {
        return instance;
    }
    private GoodTable() { }

//    private static final int TABLE_BUCKET_SIZE = 256;
//    private static final int TABLE_CACHE_SIZE = 256;
//    private static final int REOPEN_TABLE_CACHE_SIZE = 32;
//
    private static final int TABLE_BUCKET_SIZE = 256*(1<<10);
    private static final int TABLE_CACHE_SIZE = 256*(1<<10);
    private static final int REOPEN_TABLE_CACHE_SIZE = 32*(1<<10);
    // 每页的大小，单位为byte
    private static final int TABLE_PAGE_SIZE = 4*(1<<10);
    private static final String[] TABLE_COLUMNS = {"goodid"};
    public HashTable baseTable;


    // 在构造之前做初始工作
    public void init(Collection<String> storeFolders) {
        baseTable = new HashTable("goodTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        baseTable.init(storeFolders, TABLE_BUCKET_SIZE, TABLE_CACHE_SIZE, TABLE_PAGE_SIZE);
    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {
        this.baseTable.reopen(REOPEN_TABLE_CACHE_SIZE);
    }

    public HashMap<String, Object> find(String goodId) {
        return baseTable.findRecord(goodId);
    }
}
