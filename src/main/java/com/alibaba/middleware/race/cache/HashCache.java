package com.alibaba.middleware.race.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class HashCache<K, V> extends LinkedHashMap<K, V> {

    private int maxSize;

    public HashCache(int size) {
        super(size, (float)0.75, true);
        this.maxSize = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return this.size() > this.maxSize;
    }
}
