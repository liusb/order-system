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

    // 记录缓存在文件中的位置，当写入多个内容改变的缓存时排序后可以顺序写入
    protected long posInFile;

    // 缓存放置的位置
    private int posInCache;

    public abstract int getMemory();

    public abstract boolean canRemove();

    public void setPosInFile(long posInFile) {
        this.posInFile = posInFile;
    }

    public long getPosInFile() {
        return posInFile;
    }

    public void setPosInCache(int posInCache) {
        this.posInCache = posInCache;
    }

    public int getPosInCache() {
        return posInCache;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean b) {
        changed = b;
    }

    @Override
    public int compareTo(CacheObject other) {
        return MathUtils.compareLong(getPosInFile(), other.getPosInFile());
    }
}
