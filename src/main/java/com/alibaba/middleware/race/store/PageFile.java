package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.Cache;
import com.alibaba.middleware.race.cache.CacheLRU;
import com.alibaba.middleware.race.cache.CacheObject;
import com.alibaba.middleware.race.cache.CacheWriter;
import com.alibaba.middleware.race.utils.Constants;

public class PageFile implements CacheWriter {

    private final String fileName;
    private FileStore file;
    private int pageSize;
    private long pageCount;

    private final Cache cache;

    public PageFile(String fileName) {
        this.fileName = fileName;
        this.cache = CacheLRU.getCache(this, 1000);
        this.pageSize = Constants.PAGE_SIZE;
    }

    public synchronized void open() {
        try {
            this.file = new FileStore(fileName, "rw");
            this.pageCount = file.getLength()/pageSize;
        }catch (Exception e) {
        }
    }


    public synchronized Page getPage(int pageId) {
        Page p = (Page) cache.get(pageId);
        if (p != null) {
            return p;
        }
        Data data = new Data(new byte[pageSize]);
        readPage(pageId, data);
        byte type = data.readByte();
        data.readInt();
        switch (type) {
            case Page.TYPE_DATA_HASH:
                p = HashDataPage.read(data, pageId);
                break;
            default:
                System.err.println("ERROR: page=" + pageId + ", type not support :" + type);
        }
        cache.put(p);
        return p;
    }

    void readPage(int pos, Data page) {
        file.seek((long) pos << 4);
        file.read(page.getBytes(), 0, pageSize);
    }

    public Page allocatePage() {
        return null;
    }

    @Override
    public synchronized void writeBack(CacheObject object) {
        Page page = (Page)object;
        page.write();
    }
}
