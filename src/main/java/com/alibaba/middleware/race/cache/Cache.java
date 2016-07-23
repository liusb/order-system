package com.alibaba.middleware.race.cache;

import java.util.ArrayList;

public interface Cache {

    void clear();

    CacheObject find(int pos);

    CacheObject get(int pos);

    void put(CacheObject obj);

    CacheObject update(int pos, CacheObject obj);

    boolean remove(int pos);

    void removeFromLinkedList(CacheObject obj);

    void setMaxMemory(int size);

    int getMaxMemory();

    int getMemory();

    ArrayList<CacheObject> getAllChanged();

}
