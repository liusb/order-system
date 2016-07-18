package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.utils.Constants;

import java.util.ArrayList;
import java.util.Collections;

public class CacheLRU implements Cache {

    // 缓存链的链头
    private final CacheObject head = new CacheHead();

    // 桶数量
    private final int len;
    private final int mask;
    // 哈希桶第一个对象地址
    private CacheObject[] values;

    // 最大的内存使用
    private int maxMemory;
    // 当前内存使用
    private int memory;

    // 负责Cache对象的写回
    private final CacheWriter writer;

    public CacheLRU(CacheWriter writer, int cacheSize) {
        this.writer = writer;
        this.maxMemory = cacheSize*Constants.PAGE_SIZE;
        this.memory = 0;
        this.len = cacheSize;
        this.mask = len-1;
        clear();
    }

    // 清空缓存
    @Override
    public void clear() {
        head.next = head.previous = head;
        values = null;  // 先释放，直接new可能导致out of memory
        values = new CacheObject[len];
        memory = len*8;  // 指针所占用内存
    }

    @Override
    public CacheObject find(int posInCache) {
        CacheObject obj = values[posInCache & mask];
        while (obj != null && obj.getPosInCache() != posInCache) {
            obj = obj.cacheChained;
        }
        return obj;
    }

    // 获取对象，并修改其位置
    @Override
    public CacheObject get(int posInCache) {
        CacheObject obj = find(posInCache);
        if (obj != null) {
            removeFromLinkedList(obj);
            addToFront(obj);
        }
        return obj;
    }

    @Override
    public void put(CacheObject obj) {
        int posInCache = obj.getPosInCache();
        CacheObject old = find(posInCache);
        if (old != null) {
            System.err.println("ERROR: try to add a record twice at posInCache " + posInCache);
        }
        int index = obj.getPosInCache() & mask;
        obj.cacheChained = values[index];
        values[index] = obj;
        memory += obj.getMemory();
        addToFront(obj);
        removeOldIfRequired();
    }

    @Override
    public CacheObject update(int posInCache, CacheObject obj) {
        CacheObject old = find(posInCache);
        if (old == null) {
            put(obj);
        } else {
            if (old != obj) {
                System.err.println("ERROR: old!=record posInCache:" + posInCache + " old:" + old + " new:" + obj);
            }
            removeFromLinkedList(obj);
            addToFront(obj);
        }
        return old;
    }

    @Override
    public boolean remove(int posInCache) {
        int index = posInCache & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return false;
        }
        if (rec.getPosInCache() == posInCache) {
            values[index] = rec.cacheChained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.cacheChained;
                if (rec == null) {
                    return false;
                }
            } while (rec.getPosInCache() != posInCache);
            last.cacheChained = rec.cacheChained;
        }
        memory -= rec.getMemory();
        removeFromLinkedList(rec);
        rec.cacheChained = null;
        return true;
    }

    @Override
    public void setMaxMemory(int size) {
        maxMemory = size;
    }

    @Override
    public int getMaxMemory() {
        return maxMemory;
    }

    @Override
    public int getMemory() {
        return memory;
    }


    private void addToFront(CacheObject rec) {
        rec.next = head;
        rec.previous = head.previous;
        rec.previous.next = rec;
        head.previous = rec;
    }

    private void removeFromLinkedList(CacheObject rec) {
        rec.previous.next = rec.next;
        rec.next.previous = rec.previous;
        rec.next = null;
        rec.previous = null;
    }

    private void removeOldIfRequired() {
        // a small method, to allow inlining
        if (memory >= maxMemory) {
            removeOld();
        }
    }

    private void removeOld() {
        ArrayList<CacheObject> changed = new ArrayList<CacheObject>();
        int mem = memory;
        CacheObject next = head.next;
        while (true) {
            if (mem <= maxMemory) {
                break;
            }
            CacheObject check = next;
            next = check.next;
            // we are not allowed to remove it or write it if the record is pinned
            if (!check.canRemove()) {
                removeFromLinkedList(check);
                addToFront(check);
                continue;
            }
            mem -= check.getMemory();
            if (check.isChanged()) {
                changed.add(check);
            } else {
                remove(check.getPosInCache());
            }
        }
        if (changed.size() > 0) {
            Collections.sort(changed);
            int size = changed.size();
            for (int i = 0; i < size; i++) {
                CacheObject rec = changed.get(i);
                writer.writeBack(rec);
                remove(rec.getPosInCache());
            }
        }
    }

    @Override
    public ArrayList<CacheObject> getAllChanged() {
        ArrayList<CacheObject> changed = new ArrayList<CacheObject>();
        CacheObject rec = head.next;
        while (rec != head) {
            if (rec.isChanged()) {
                changed.add(rec);
            }
            rec = rec.next;
        }
        return changed;
    }

}
