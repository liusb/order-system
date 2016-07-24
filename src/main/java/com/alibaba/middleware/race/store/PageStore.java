package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.*;
import com.alibaba.middleware.race.index.HashIndex;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

public class PageStore implements CacheWriter {

    // 记录空页
    private BitSet usedSet;
    private final String fileName;
    private final int bucketSize;
    private final int pageSize;
    private int nextOverflowPageId;

    private FileStore file;
    private String mode;
    private Cache cache;

    public PageStore(String fileName, int bucketSize, int pageSize) {
        this.fileName = fileName;
        this.bucketSize = bucketSize;
        this.nextOverflowPageId = bucketSize;
        this.pageSize = pageSize;
    }

    public void open(String mode, int cacheSize) {
        this.mode = mode;
        try {
            this.file = new FileStore(fileName, mode);
            this.cache = new CacheLRU(this, cacheSize, pageSize);
            if (mode.equals("rw")) {
                this.usedSet = new BitSet(bucketSize);  // 初始化为bucketSize大小，然后随着页的分配增加
                this.file.setLength(bucketSize*pageSize*100);  // 需要预估计分配大小，应该比所有桶大小更大
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

    // 此方法为读取用
    public synchronized HashDataPage getPage(int pageId) {
        HashDataPage p = (HashDataPage) cache.get(pageId);
        if (p != null) {
            return p;
        }
        Data data = new Data(new byte[pageSize]);
        file.seek(((long) pageId) * pageSize);
        file.read(data.getBytes(), 0, pageSize);
        HashDataPage page = new HashDataPage(data, pageId);
        page.parseHeader();
        cache.put(page);
        return page;
    }



    // 此方法为写入用
    private HashDataPage loadBucketPage(int pageId) {
        Data data = new Data(new byte[pageSize]);
        file.seek(((long) pageId) * pageSize);
        file.read(data.getBytes(), 0, pageSize);
        HashDataPage page = new HashDataPage(data, pageId);
        page.parseHeader();
        data.setPos(page.getDataLen());
        page.setChanged(true);
        cache.put(page);
        return page;
    }

    // 获取桶中首个可以写入数据的页
    private HashDataPage getBucket(int bucketId) {
        HashDataPage page = (HashDataPage)cache.get(bucketId);
        if (page != null) {  // 1. 在缓存中
            if (!page.dataIsFree()) {  // 1.1 没有写满，直接返回
                return page;
            } else  { // 1.2 桶已经溢出, 指向目前可以写入的页
                int nextPageId = page.getNextPage();
                page = (HashDataPage)cache.get(nextPageId);
                if (page == null) {  // 1.2.1 页已经被置换出去，重新读入
                    if (!usedSet.get(nextPageId)) {
                        throw new RuntimeException("页面被置换，却没分配不合逻辑");
                    }
                    page = loadBucketPage(nextPageId);
                }
                return page;
            }
        } else if(usedSet.get(bucketId)){   // 2. 不在缓存中，但已经分配过，那就从磁盘读入，并放入缓存
            return loadBucketPage(bucketId);
        } else {   // 3. 不在缓存中，而且没有分配过，那就分配并放入缓存
            Data data = new Data(new byte[pageSize]);
            page = new HashDataPage(data, bucketId);
            usedSet.set(bucketId);
            page.setChanged(true);
            cache.put(page);
            return page;
        }
    }

    // 向桶中添加页
    private HashDataPage expandBucket(HashDataPage oldPage, int bucketId) {
        int newPageId = (this.nextOverflowPageId++);
        Data oldData = oldPage.getData();
        // 修改旧页的下一页表项, 将改页写入文件
        oldPage.setNextPage(newPageId);
        this.writeBack(oldPage);
        if (bucketId == oldPage.getPageId()) { // 桶中第一个页溢出
            // 将该页从缓存链表中移出
            cache.removeData(oldPage);
        } else {  // 不是第一个溢出的桶, 找到第一页，指向新的溢出页
            HashDataPage firstPage = (HashDataPage)cache.find(bucketId);
            firstPage.setNextPage(newPageId);
            cache.remove(oldPage.getPos());
        }
        oldPage.freeData();
        HashDataPage newPage = new HashDataPage(oldData, newPageId);
        usedSet.set(newPageId);
        newPage.setChanged(true);
        cache.put(newPage);
        return newPage;
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
                copyPos += emptyLength;
                page = expandBucket(page, bucketId);
                data = page.getData();
            } else {
                data.copyFrom(buffer, copyPos, buffer.getPos()-copyPos);
                if (emptyLength == buffer.getPos()-copyPos) { // 正好写满，分配新页
                    expandBucket(page, bucketId);
                }
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
            for (CacheObject rec: changed) {
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
        long writePos = ((long)page.getPos())*pageSize;
        file.seek(writePos);
        if (writePos + pageSize < file.getLength()) {
            file.write(data.getBytes(), 0, dataLen);
        }else {
            file.write(data.getBytes(), 0, pageSize);
        }
        page.setChanged(false);
    }



    public long FileCheck() {
        long recordCount = 0;
        for (int i=0; i<this.bucketSize; i++) {
            int pageId = i;
            if (!usedSet.get(i)) {
                continue;
            }
            int readHash=0;
            int readLen=0;
            int dataLen = 0;
            HashDataPage page;
            Data data = new Data(new byte[1]);
            Data buffer = SafeData.getData();
            boolean nextRecord = true;
            while (true) {
                if (dataLen == data.getPos()) {
                    if(pageId == -1) {
                        break;
                    }
                    page = getPage(pageId);
                    data = page.getData();
                    dataLen = page.getDataLen();
                    pageId = page.getNextPage();
                }
                if (nextRecord) {
                    readHash = data.readInt();
                    readLen = data.readInt();
                    buffer.reset();
                    recordCount ++;
                }
                int copyLen = Math.min(readLen - buffer.getPos(), dataLen - data.getPos());
                buffer.copyFrom(data, data.getPos(), copyLen);
                data.setPos(data.getPos()+copyLen);
                if (readLen == buffer.getPos()) {
                    // 验证
                    buffer.reset();
                    String key = buffer.readString();
                    if (readHash != HashIndex.getHashCode(key)) {
                        throw new RuntimeException("数据错误");
                    }
                    nextRecord = true;
                } else {
                    nextRecord = false;
                }
            }
        }
        System.out.println("recordCount:" + recordCount);
        return recordCount;
    }

}
