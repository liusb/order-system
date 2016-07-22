package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.Cache;
import com.alibaba.middleware.race.cache.CacheLRU;
import com.alibaba.middleware.race.cache.CacheObject;
import com.alibaba.middleware.race.cache.CacheWriter;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

public class PageStore implements CacheWriter {

    // 记录空页
    private final BitSet usedSet;
    private final String fileName;
    private final int bucketSize;
    private final int pageSize;

    private FileStore file;
    private String mode;
    private Cache cache;

    public PageStore(String fileName, int bucketSize, int pageSize) {
        this.fileName = fileName;
        this.usedSet = new BitSet(bucketSize);
        this.bucketSize = bucketSize;
        this.pageSize = pageSize;
    }

    public void open(String mode, int cacheSize) {
        this.mode = mode;
        try {
            this.file = new FileStore(fileName, mode);
            this.cache = new CacheLRU(this, cacheSize, pageSize);
            if (mode.equals("rw")) {
                this.file.setLength(bucketSize*pageSize);
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public void close() {
        try {
            if (mode.equals("rw")) {
                this.writeAllChanged();
            }
            this.file.close();
            this.file = null;
            this.cache = null;
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public synchronized HashDataPage getPage(int pageId) {
        HashDataPage p = (HashDataPage) cache.get(pageId);
        if (p != null) {
            return p;
        }
        Data data = new Data(new byte[pageSize]);
        readPageData(data, pageId);
        p = new HashDataPage(data, pageId);
        p.readHeader();
        cache.put(p);
        return p;
    }

    void readPageData(Data page, int pageId) {
        file.seek(((long) pageId) * pageSize);
        file.read(page.getBytes(), 0, pageSize);
    }

    // 分配第一个桶
    private HashDataPage allocateBucket(int pageId) {
        if (usedSet.get(pageId)) {
            throw new RuntimeException("重复分配了页面，可能缓存被替换了。pageId: "+pageId);
        }
        Data data = new Data(new byte[pageSize]);
        HashDataPage page = new HashDataPage(data, pageId);
        page.setChanged(true);
        page.setPos(pageId);
        usedSet.set(pageId);
        cache.put(page);
        return page;
    }

    // 获取桶中首个可以写入数据的页
    private HashDataPage getBucket(int pageId) {
        HashDataPage page = (HashDataPage)cache.get(pageId);
        if (page != null) {
            while (page.dataIsFree()) {
                // todo 此次应该优化成只有一次的链接
                page = (HashDataPage)cache.get(page.getNextPage());
            }
        } else {
            page = allocateBucket(pageId);
        }
        return page;
    }

    // 向桶中添加页
    private HashDataPage expandBucket(HashDataPage oldPage) {
        int oldPageId = oldPage.getPageId();
        Data oldData = oldPage.getData();
        long oldLength = file.getLength();
        file.setLength(oldLength + pageSize);
        int newPageId = (int) (oldLength / pageSize);
        // 修改旧页的下一页表项, 将改页写入文件
        oldPage.setNextPage(newPageId);
        this.writeBack(oldPage);
        oldPage.freeData();
        HashDataPage newPage = new HashDataPage(oldData, newPageId);
        newPage.setPreviousPage(oldPageId);
        return newPage;
    }

    public long insertData(int pageId, Data buffer) {
        HashDataPage page = getBucket(pageId);
        Data data = page.getData();
        if (data.getLength() - data.getPos() < 8) {  // 剩余空间太小，不足写入hashCode和length
            // 分配新页， 写入旧页
            page = expandBucket(page);
            data = page.getData();
        }
        long address = ((long)pageId*pageSize + data.getPos());  // 返回记录写入的地址
        int bufferPos = 0;
        while (bufferPos < buffer.getPos()) {
            int leftLength = data.getLength() - data.getPos();
            if (leftLength < buffer.getPos() - bufferPos) {  // 写不下全部，写入一部分
                // 写入数据
                System.arraycopy(buffer.getBytes(), bufferPos, data.getBytes(), data.getPos(), leftLength);
                bufferPos = buffer.getPos() - leftLength;
                page = expandBucket(page);
                data = page.getData();
            } else {
                System.arraycopy(buffer.getBytes(), bufferPos, data.getBytes(), data.getPos(), buffer.getPos()-bufferPos);
                bufferPos = buffer.getPos();
            }
        }
        return address;
    }

    public void insertIndexData(int pageId, Data buffer) {
        // 判断是否能够写入该页， 不能写入直接写到下一页
        HashDataPage page = getBucket(pageId);
        Data data = page.getData();
        if (data.getLength() - data.getPos() < buffer.getPos()) {
            page = expandBucket(page);
            data = page.getData();
        }
        data.copyFrom(buffer, 0, buffer.getPos());
    }

    public void writeAllChanged() {
        ArrayList<CacheObject> changed = cache.getAllChanged();
        if (changed.size() > 0) {
            Collections.sort(changed);
            int size = changed.size();
            for (int i = 0; i < size; i++) {
                CacheObject rec = changed.get(i);
                this.writeBack(rec);
            }
        }
    }

    @Override
    public void writeBack(CacheObject object) {
        Page page = (Page)object;
        Data data = page.getData();
        int dataLen = data.getPos();
        page.setDataLen(dataLen);
        page.writeHeader();
        file.seek(((long)page.getPos())*pageSize);
        file.write(data.getBytes(), 0, dataLen);
        page.setChanged(false);
    }
}
