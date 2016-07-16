package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.store.PageFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class HashTable extends Table {
    private HashIndex index;

    public HashTable(String name) {
        this.columns = new HashMap<String, Column>();
        this.storeFiles = new ArrayList<PageFile>();
        this.name = name;
    }

    public void init(Collection<String> storeFolders, String key) {
        for (String folder: storeFolders) {
            this.storeFiles.add(new PageFile(folder + this.name + ".db"));
        }
        Column column = new Column(key, 0);
        this.columns.put(key, column);
    }

    public void setIndex(HashIndex index) {
        this.index = index;
    }
}
