package com.alibaba.middleware.race.cache;

import java.util.HashMap;

public class IndexCache {

    private HashMap<Long, IndexEntry> cache;

    public IndexCache(int size) {
        this.cache = new HashMap<Long, IndexEntry>(size);
    }

    public synchronized void put(Long key, IndexEntry entry) {
        IndexEntry indexEntry = cache.get(key);
        if (indexEntry == null) {
            cache.put(key, entry);
        }
    }

    public IndexEntry get(Long key) {
        IndexEntry entry = cache.get(key);
        return entry;
    }

}
