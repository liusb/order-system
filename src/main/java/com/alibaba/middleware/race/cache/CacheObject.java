package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.utils.MathUtils;

public abstract class CacheObject implements Comparable<CacheObject> {

    // 记录移出缓存顺序的链表指针
    public CacheObject previous;
    public CacheObject next;

    // 哈希冲突链接指针
    public CacheObject cacheChained;

    // 记录缓存是否被修改
    private boolean changed;

    // 缓存放置的位置
    private int pos;

    public abstract int getMemory();

    public abstract boolean canRemove();

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean b) {
        changed = b;
    }

    @Override
    public int compareTo(CacheObject other) {
        return MathUtils.compareLong(getPos(), other.getPos());
    }
}
