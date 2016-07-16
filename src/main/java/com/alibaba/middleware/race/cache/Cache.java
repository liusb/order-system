package com.alibaba.middleware.race.cache;

public interface Cache {

    void clear();

    CacheObject find(int pos);

    CacheObject get(int pos);

    void put(CacheObject obj);

    CacheObject update(int pos, CacheObject obj);

    boolean remove(int pos);


    void setMaxMemory(int size);

    int getMaxMemory();

    int getMemory();

}
