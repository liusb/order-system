package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.store.PageStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class HashTable extends Table {
    private HashIndex index;

    public HashTable(String name) {
        this.columns = new HashMap<String, Column>();
        this.storeFiles = new ArrayList<PageStore>();
        this.name = name;
    }

    public void init(Collection<String> storeFolders, String key, int cacheSize) {
        for (String folder: storeFolders) {
            PageStore pageStore = new PageStore(folder + "/" + this.name + ".db", cacheSize);
            pageStore.open();
            this.storeFiles.add(pageStore);
        }
        Column column = new Column(key, 0);
        this.columns.put(key, column);
    }

    public void setIndex(HashIndex index) {
        this.index = index;
    }

}
