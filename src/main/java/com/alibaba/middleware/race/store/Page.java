package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.CacheObject;

public abstract class Page extends CacheObject {

    public static final byte TYPE_DATA_HASH = 1;

    // data 用来换成页的数据，每页第一个byte表示页的类型
    protected Data data;
    // 为data在文件中的起始位置除以页长，所以从0开始计数
    protected int pageId;

    protected int previousPage;
    protected int nextPageId;
    protected int dataLength;

    public abstract Page nextPage();
    public abstract Page previousPage();

    @Override
    public int getMemory() {
        return data.getBytes().length;
    }

    public abstract void write();
}
