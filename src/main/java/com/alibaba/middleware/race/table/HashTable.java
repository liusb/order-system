package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.store.PageStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class HashTable extends Table {
    private HashIndex index;
    private int hashColumnId;

    public HashTable(String name) {
        this.storeFiles = new ArrayList<PageStore>();
        this.name = name;
        this.hashColumnId = 0;
    }

    public void init(Collection<String> storeFolders, int bucketSize,
                     int cacheSize, int pageSize) {
        for (String folder: storeFolders) {
            PageStore pageStore = new PageStore(folder + "/" + this.name + ".db",
                    bucketSize, pageSize);
            pageStore.open("rm", cacheSize);
            this.storeFiles.add(pageStore);
        }
        this.index = new HashIndex(bucketSize, this.storeFiles.size());
    }

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Column>();
        int columnsId = 0;
        for (String key: columnsKeys) {
            Column column = new Column(key, columnsId);
            this.columns.put(key, column);
            columnsId++;
        }
    }

    public void setBaseColumns(HashMap<String, Column> baseColumns) {
        this.columns = baseColumns;
    }

    public HashIndex getIndex() {
        return this.index;
    }

    public void setHashColumnId(String key) {
        this.hashColumnId = this.columns.get(key).getColumnId();
    }
}
