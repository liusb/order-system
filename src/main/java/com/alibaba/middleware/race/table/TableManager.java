package com.alibaba.middleware.race.table;


public class TableManager {
    private static TableManager ourInstance = new TableManager();

    private TableManager() { }

    public static TableManager instance() {
        return ourInstance;
    }

    public HashTable goodTable = new HashTable("goodTable");
    public HashTable buyerTable = new HashTable("buyerTable");
}
