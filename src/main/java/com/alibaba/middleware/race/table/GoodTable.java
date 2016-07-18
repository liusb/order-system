package com.alibaba.middleware.race.table;

import java.util.HashMap;
import java.util.TreeMap;

public class GoodTable extends HashTable {

    private static GoodTable instance = new GoodTable();
    public static GoodTable getInstance() {
        return instance;
    }

    private GoodTable() {
        super("good");
    }

    // 在构造之前做初始工作
    public void init() {

    }

    // 在构造完，准备查询前重新打开
    public void open(String mode) {

    }

    public HashMap<String, Object> find(String goodId, TreeMap<Integer, String> keys) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        return result;
    }
}
