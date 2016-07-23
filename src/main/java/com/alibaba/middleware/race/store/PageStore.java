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
    private BitSet usedSet;
    private final String fileName;
    private final int bucketSize;
    private final int pageSize;

    private FileStore file;
    private String mode;
    private Cache cache;

    public PageStore(String fileName, int bucketSize, int pageSize) {
        this.fileName = fileName;
        this.bucketSize = bucketSize;
        this.pageSize = pageSize;
    }

    public void open(String mode, int cacheSize) {
        this.mode = mode;
        try {
            this.file = new FileStore(fileName, mode);
            this.cache = new CacheLRU(this, cacheSize, pageSize);
            if (mode.equals("rw")) {
                this.usedSet = new BitSet(bucketSize);  // 初始化为bucketSize大小，然后随着页的分配增加
                this.file.setLength(bucketSize*pageSize);   // todo 需要预估计分配大小，应该比所有桶大小更大
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
        return loadPage(pageId);
    }

    private HashDataPage loadPage(int pageId) {
        Data data = new Data(new byte[pageSize]);
        HashDataPage page = new HashDataPage(data, pageId);
        file.seek(((long) pageId) * pageSize);
        file.read(page.getData().getBytes(), 0, pageSize);
        page.readHeader();
        cache.put(page);
        return page;
    }

    // 分配第一个桶
    private HashDataPage allocateBucket(int bucketId) {
        if (usedSet.get(bucketId)) {
            return loadPage(bucketId);
        } else {   // 分配新桶
            Data data = new Data(new byte[pageSize]);
            HashDataPage page = new HashDataPage(data, bucketId);
            page.setChanged(true);
            page.setPos(bucketId);
            usedSet.set(bucketId);
            cache.put(page);
            return page;
        }
    }

    // 获取桶中首个可以写入数据的页
    private HashDataPage getBucket(int bucketId) {
        HashDataPage page = (HashDataPage)cache.get(bucketId);
        if (page != null) {
            if (page.dataIsFree()) { // 桶已经溢出, 指向目前可以写入的页
                int nextPageId = page.getNextPage();
                page = (HashDataPage)cache.get(nextPageId);
                if (page == null) {  // 页已经被置换出去，重新读入
                    page = loadPage(nextPageId);
                }
            }
        } else {
            page = allocateBucket(bucketId);
        }
        return page;
    }

    // 向桶中添加页
    private HashDataPage expandBucket(HashDataPage oldPage, int bucketId) {
        if (bucketId == oldPage.getPageId()) { // 桶中第一个页溢出
            Data oldData = oldPage.getData();
            int newPageId = usedSet.length();
            usedSet.set(newPageId);
            file.setLength(file.getLength() + pageSize);
            // 修改旧页的下一页表项, 将改页写入文件
            oldPage.setNextPage(newPageId);
            this.writeBack(oldPage);
            oldPage.freeData();
            // 将该页从缓存链表中移出
            cache.removeFromLinkedList(oldPage);
            return new HashDataPage(oldData, newPageId);
        } else {  // 不是第一个溢出的桶
            Data oldData = oldPage.getData();
            long oldLength = file.getLength();
            file.setLength(oldLength + pageSize);
            int newPageId = (int) (oldLength / pageSize);
            // 修改旧页的下一页表项, 将改页写入文件
            oldPage.setNextPage(newPageId);
            this.writeBack(oldPage);
            oldPage.freeData();
            HashDataPage firstPage = (HashDataPage)cache.find(bucketId);
            firstPage.setNextPage(newPageId);
            return new HashDataPage(oldData, newPageId);
        }
    }

    public long insertData(int bucketId, Data buffer) {
        HashDataPage page = getBucket(bucketId);   // 获取桶中可写入的页
        Data data = page.getData();
        if (data.getLength() - data.getPos() < 8) {  // 剩余空间太小，不足写入hashCode和length
            // 分配新页， 写入旧页
            page = expandBucket(page, bucketId);
            data = page.getData();
        }
        long address = ((long)page.getPageId()*pageSize + data.getPos());  // 返回记录写入的地址
        int copyPos = 0;
        while (true) {
            int emptyLength = data.getLength() - data.getPos();
            if (emptyLength < buffer.getPos() - copyPos) {  // 写不下全部，写入一部分
                // 写入数据
                data.copyFrom(buffer, copyPos, emptyLength);
                copyPos = buffer.getPos() - emptyLength;
                page = expandBucket(page, bucketId);
                data = page.getData();
            } else {
                data.copyFrom(buffer, copyPos, buffer.getPos()-copyPos);
                break;
            }
        }
        return address;
    }

    public void insertIndexData(int bucketId, Data buffer) {
        // 判断是否能够写入该页， 不能写入直接写到下一页
        HashDataPage page = getBucket(bucketId);
        Data data = page.getData();
        if (data.getLength() - data.getPos() < buffer.getPos()) {
            page = expandBucket(page, bucketId);
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
