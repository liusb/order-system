package com.alibaba.middleware.race.cache;

import java.lang.ref.SoftReference;
import java.util.LinkedHashMap;
import java.util.Map;

public class TwoLevelCache<K, V> {

    private final LinkedHashMap<K, V> firstLevelCache;
    private final LinkedHashMap<K, SoftReference<V>>  secondLevelCache;

    public TwoLevelCache(final int firstLevelSize, final int secondLevelSize) {

        this.firstLevelCache = new LinkedHashMap<K, V>(firstLevelSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (size() > firstLevelSize) {
                    secondLevelCache.put(eldest.getKey(), new SoftReference<V>(eldest.getValue()));
                    return true;
                } else {
                    return false;
                }
            }
        };

        this.secondLevelCache = new LinkedHashMap<K, SoftReference<V>>(secondLevelSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, SoftReference<V>> eldest) {
                return this.size() > secondLevelSize;
            }
        };
    }

    public V get(K key) {
        V value;
        synchronized (firstLevelCache) {
            value = firstLevelCache.get(key);
            if (value != null) {
                return value;
            }
        }
        synchronized (secondLevelCache) {
            SoftReference<V> softValue = secondLevelCache.get(key);
            if (softValue != null) {
                return softValue.get();
            } else {
                secondLevelCache.remove(key);
            }
        }
        return null;
    }

    public void put(K key, V value) {
        synchronized (firstLevelCache) {
            firstLevelCache.put(key, value);
        }
    }

}
