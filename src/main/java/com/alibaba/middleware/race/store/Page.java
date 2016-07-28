package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.CacheObject;

public abstract class Page extends CacheObject {

    protected int dataLen;

    protected Data data;

    public Page(Data data, int dataLen) {
        this.data = data;
        this.dataLen = dataLen;
        this.data.setPos(dataLen);
    }

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
