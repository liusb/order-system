package com.alibaba.middleware.race.store;

import com.alibaba.middleware.race.cache.Cache;
import com.alibaba.middleware.race.cache.CacheLRU;
import com.alibaba.middleware.race.cache.CacheObject;
import com.alibaba.middleware.race.cache.CacheWriter;
import com.alibaba.middleware.race.utils.Constants;

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
            this.cache = new CacheLRU(this, cacheSize);
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
            this.cache.clear();
            this.file = null;
            this.cache = null;
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public synchronized Page getPage(int pageId) {
        Page p = (Page) cache.get(pageId);
        if (p != null) {
            return p;
        }
        Data data = new Data(new byte[pageSize]);
        //readPage(pageId, data);
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

    private Page allocateNewPage(Data data, int pageId) {
        HashDataPage page = new HashDataPage(data, pageId);
        page.writeDefaultHeader();
        page.setChanged(true);
        page.setPosInCache(pageId);
        usedSet.set(pageId);
        return page;
    }

    public Page allocateBucketPage(int pageId) {
        HashDataPage page;
        Data data = new Data(new byte[pageSize]);
        if (usedSet.get(pageId)) {  // 已经分配过， 说明页溢出
            long oldLength = file.getLength();
            file.setLength(oldLength + Constants.PAGE_SIZE);
            page = new HashDataPage(data, (int)(oldLength/Constants.PAGE_SIZE));
            page.setPosInCache(pageId);
            // 修改旧页的下一页表项, 将改页写入文件
            Page oldPage = (Page)cache.get(pageId);
            cache.remove(pageId);
            oldPage.getData().setInt(HashDataPage.NextPos, page.getPageId());
            this.writeBack(oldPage);
            page.getData().setInt(HashDataPage.PreviousPos, oldPage.getPageId());
            page.getData().setInt(HashDataPage.NextPos, -1);
        } else {  // 未分配，分配新页
            page = (HashDataPage)allocateNewPage(data, pageId);
        }
        cache.put(page);
        return page;
    }

    public long insertData(int pageId, Data buffer) {
        long address = 0;
        Page page = (Page)cache.get(pageId);
        if (page == null) {
            if (usedSet.get(pageId)) {  // 已经分配过但被置换出
                throw new RuntimeException("TODO: 现在还不支持置换: " + pageId);
            } else {
                page = allocateBucketPage(pageId);
            }
        }
        Data data = page.getData();
        if (data.getLength() - data.getPos() > 8) {  // 剩余空间太小，不足写入hashCode和length
            // 分配新页， 写入旧页
            page = allocateBucketPage(pageId);
            data = page.getData();
        }
        int bufferPos = 0;
        while (bufferPos < buffer.getPos()) {
            int leftLength = data.getLength() - data.getPos();
            if (leftLength < buffer.getPos() - bufferPos) {  // 写不下全部，写入一部分
                // 写入数据
                System.arraycopy(buffer.getBytes(), bufferPos, data.getBytes(), data.getPos(), leftLength);
                bufferPos = buffer.getPos() - leftLength;
                page = allocateBucketPage(pageId);
                data = page.getData();
            } else {
                System.arraycopy(buffer.getBytes(), bufferPos, data.getBytes(), data.getPos(), buffer.getPos()-bufferPos);
                bufferPos = buffer.getPos();
            }
        }
        return address;
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
        file.seek(page.getPosInFile());
        file.write(data.getBytes(), 0, dataLen);
        page.setChanged(false);
    }
}
