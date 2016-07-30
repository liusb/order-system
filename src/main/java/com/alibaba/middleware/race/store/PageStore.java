package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.*;
import com.alibaba.middleware.race.index.HashIndex;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

public class PageStore {

    // 记录空页
    private BitSet usedSet;
    private final String fileName;
    private final int bucketSize;
    private final int pageSize;
    private int nextOverflowPageId;

    private FileStore file;
    private String mode;
    private DataPage[] pageCache;
    private ArrayList<DataPage> pagesWaitToWrite;

    public PageStore(String fileName, int bucketSize, int pageSize) {
        this.fileName = fileName;
        this.bucketSize = bucketSize;
        this.nextOverflowPageId = bucketSize;
        this.pageSize = pageSize;
    }

    public void open(String mode) {
        this.mode = mode;
        try {
            this.file = new FileStore(fileName, mode);
            if (mode.equals("rw")) {
                this.pageCache = new DataPage[bucketSize];
                this.pagesWaitToWrite = new ArrayList<DataPage>();
                this.usedSet = new BitSet(bucketSize);  // 初始化为bucketSize大小，然后随着页的分配增加
                this.file.setLength(((long)bucketSize)*pageSize*10);  // 需要预估计分配大小，应该比所有桶大小更大
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
            this.pageCache = null;
            this.pagesWaitToWrite = null;
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    // 此方法为读取用
    public synchronized DataPage getPage(int pageId) {
        Data data = new Data(new byte[pageSize]);
        file.seek(((long) pageId) * pageSize);
        file.read(data.getBytes(), 0, pageSize);
        DataPage page = new DataPage(data, pageId);
        page.parseHeader();
        return page;
    }


    public boolean isBucketUsed(int bucketId) {
        return this.usedSet.get(bucketId);
    }

    // 获取桶中首个可以写入数据的页
    private DataPage getBucket(int bucketId) {
        if (pageCache[bucketId] == null) {
            pageCache[bucketId] = new DataPage(new Data(new byte[pageSize]), bucketId);
            usedSet.set(bucketId);
        }
        return pageCache[bucketId];
    }

    // 向桶中添加页
    private DataPage expandBucket(DataPage oldPage, int bucketId) {
        int newPageId = (this.nextOverflowPageId++);
        // 修改旧页的下一页表项, 将该页写入文件
        oldPage.setNextPage(newPageId);
        pagesWaitToWrite.add(oldPage);
        this.checkCache();
        DataPage newPage = new DataPage(new Data(new byte[pageSize]), newPageId);
        pageCache[bucketId] = newPage;
        usedSet.set(newPageId);
        return newPage;
    }

    public long insertData(int bucketId, Data buffer) {
        DataPage page = getBucket(bucketId);   // 获取桶中可写入的页
        Data data = page.getData();
        if (data.getEmptySize() < 8) {  // 剩余空间太小，不足写入hashCode和length
            // 分配新页， 写入旧页
            page = expandBucket(page, bucketId);
            data = page.getData();
        }
        long address = ((long)page.getPageId())*pageSize + data.getPos();  // 返回记录写入的地址
        int copyPos = 0;
        while (true) {
            int emptyLength = data.getEmptySize();
            if (copyPos + emptyLength < buffer.getPos()) {  // 写不下全部，写入一部分
                // 写入数据
                data.copyFrom(buffer, copyPos, emptyLength);
                copyPos += emptyLength;
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
        DataPage page = getBucket(bucketId);
        Data data = page.getData();
        if (data.getLength() - data.getPos() < buffer.getPos()) {
            page = expandBucket(page, bucketId);
            data = page.getData();
        }
        data.copyFrom(buffer, 0, buffer.getPos());
    }

    public void writeAllChanged() {
        ArrayList<DataPage> changed = new ArrayList<DataPage>(bucketSize);
        for (DataPage page: this.pageCache) {
            if (page != null) {
                changed.add(page);
            }
        }
        for (DataPage page: pagesWaitToWrite) {
            changed.add(page);
        }
        Collections.sort(changed);
        for (DataPage page: changed) {
            writeBack(page);
        }
    }

    public void writeBack(DataPage page) {
        Data data = page.getData();
        int dataLen = data.getPos();
        page.setDataLen(dataLen);
        page.writeHeader();
        long writePos = ((long)page.getPageId())*pageSize;
        file.seek(writePos);
        if (writePos + pageSize < file.getLength()) {
            file.write(data.getBytes(), 0, dataLen);
        }else {
            file.write(data.getBytes(), 0, pageSize);
        }
    }

    public void aioWrite(DataPage page) {
        Data data = page.getData();
        int dataLen = data.getPos();
        page.setDataLen(dataLen);
        page.writeHeader();
        long writePos = ((long)page.getPageId())*pageSize;
        file.aioWrite(data.getBytes(), writePos);
    }

    public void checkCache() {
        if (pagesWaitToWrite.size() >= 512) {
            Collections.sort(pagesWaitToWrite);
            for (DataPage page: pagesWaitToWrite) {
                this.aioWrite(page);
            }
            pagesWaitToWrite.clear();
        }
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
            DataPage page;
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
                    data.setPos(DataPage.HeaderLength);
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
                if (data.getPos()+copyLen > data.getLength()) {
                    throw new RuntimeException("超出数组长度");
                }
                if (buffer.getPos()+copyLen > buffer.getLength()) {
                    throw new RuntimeException("超出数组长度");
                }
                if (copyLen < 0) {
                    throw new RuntimeException("超出数组长度");
                }
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
