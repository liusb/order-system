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
        } else {
            entry.next = indexEntry.next;
            indexEntry.next = entry;
            System.out.println("WARN: Key 冲突:" + key);
        }
    }

    public IndexEntry get(Long key, short prefix) {
        IndexEntry entry = cache.get(key);
        while (entry != null) {
            if (entry.prefix == prefix) {
                return entry;
            }
            entry = entry.next;
        }
        return null;
    }

}
