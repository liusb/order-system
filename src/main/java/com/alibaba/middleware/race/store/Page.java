package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.CacheObject;

public abstract class Page extends CacheObject {

    public static final byte TYPE_DATA_HASH = 1;

    public static final int DataLenPos = 0;
    protected int dataLen;

    protected Data data;

    @Override
    public int getMemory() {
        return data.getBytes().length;
    }

    public abstract void writeHeader();

    public Data getData() {
        return this.data;
    }

    public int getPageId() {
        return getPos();
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

    public int getDataLen() {
        return dataLen;
    }
}
