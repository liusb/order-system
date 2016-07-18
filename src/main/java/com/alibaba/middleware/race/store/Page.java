package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.CacheObject;
import com.alibaba.middleware.race.utils.Constants;

public abstract class Page extends CacheObject {

    public static final byte TYPE_DATA_HASH = 1;


    public static final int DataLenPos = 1;

    // data 用来换成页的数据，每页第一个byte表示页的类型
    protected Data data;

    @Override
    public int getMemory() {
        return data.getBytes().length;
    }

    public Data getData() {
        return this.data;
    }

    public int getPageId() {
        return (int)(posInFile/Constants.PAGE_SIZE);
    }

    public void setDataLen(int dataLen) {
        this.data.setInt(DataLenPos, dataLen);
    }
}
