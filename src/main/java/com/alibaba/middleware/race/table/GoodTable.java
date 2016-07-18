package com.alibaba.middleware.race.table;

import java.util.HashMap;
import java.util.TreeMap;

public class GoodTable extends HashTable {
    // config
    // 哈希桶的总个数
    private final int HASH_BUCKET_SIZE = 1024;
    // 建立表示很LRU缓存的大小
    private final int BUILD_CACHE_SIZE = 1024;
    // 每页的大小，单位为byte
    private final int PAGE_SIZE = 16*(1<<10);

    private static GoodTable instance = new GoodTable();
    public static GoodTable getInstance() {
        return instance;
    }

    private GoodTable() {
        super("good");
    }

    // 在构造之前做初始工作
    public void init() {
        // 初始化文件长度为 HASH_BUCKET_SIZE * PAGE_SIZE

    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {

    }

    public HashMap<String, Object> find(String goodId, TreeMap<Integer, String> keys) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        return result;
    }
}
