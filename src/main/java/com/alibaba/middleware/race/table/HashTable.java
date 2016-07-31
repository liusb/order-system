package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.SafeData;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.DataPage;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.type.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HashTable extends Table {
    private HashIndex index;
    private String[] columnsMap;

    public HashTable(String name) {
        this.storeFiles = new ArrayList<PageStore>();
        this.name = name;
    }

    public void init(Collection<String> storeFolders, int bucketSize, int pageSize) {
        for (String folder: storeFolders) {
            PageStore pageStore = new PageStore(folder + "/" + this.name + ".db",
                    bucketSize, pageSize);
            pageStore.open("rw");
            this.storeFiles.add(pageStore);
        }
        this.index = new HashIndex(bucketSize, this.storeFiles.size());
    }

    public void reopen() {
        for (PageStore pageStore: this.storeFiles) {
            pageStore.open("r");
        }
        this.initColumnsMap();
    }

    private void initColumnsMap() {
        columnsMap = new String[columns.size()];
        for (Map.Entry<String, Integer> entry: this.columns.entrySet()) {
            columnsMap[entry.getValue()] = entry.getKey();
        }
    }

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Integer>();
        int columnsId = columns.size();
        for (String key: columnsKeys) {
            this.columns.put(key, columnsId);
            columnsId++;
        }
    }

    public HashIndex getIndex() {
        return this.index;
    }

    public ArrayList<RecordIndex> findIndex(String buyerId,  long startTime, long endTime) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        int hashCode = HashIndex.getHashCode(buyerId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return results;
        }
        String readString;
        long readTime;
        DataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            // [buyerId(string), createTime(long), address(long<<6)|fileId(byte)]
            while (data.getPos() < page.getDataLen()) {
                readString = data.readKeyString();
                if (readString.equals(buyerId)) {
                    readTime = data.readLong();
                    if (readTime >= startTime && readTime < endTime) {
                        long address = data.readLong();
                        RecordIndex result = new RecordIndex((byte)(address&0x3F), address>>6);
                        results.add(result);
                    } else {
                        data.skip(8);
                    }
                } else {
                    data.skip(8+8);
                }
            }
            bucketIndex = page.getNextPage();
            if(bucketIndex == -1) {
                break;
            }
        }
        return results;
    }

    public RecordIndex findIndex(long orderId) {
        int hashCode = HashIndex.getHashCode(orderId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return null;
        }
        DataPage page;
        Data data;
        long readOrderId;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes());
            data.setPos(DataPage.HeaderLength);
            // [orderId(long), address(long<<6)|fileId(byte)]
            while (data.getPos() < page.getDataLen()) {
                readOrderId = data.readLong();
                if (readOrderId != orderId) {
                    data.skip(8);
                } else {
                    long address = data.readLong();
                    return new RecordIndex((byte)(address&0x3F), address>>6);
                }
            }
            bucketIndex = page.getNextPage();
            if (bucketIndex == -1) {
                return null;
            }
        }
    }

    public ArrayList<RecordIndex> findIndex(String goodId) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        int hashCode = HashIndex.getHashCode(goodId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return results;
        }
        String readString;
        DataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            // [goodId(string), address(long<<6)|fileId(byte)]
            while (data.getPos() < page.getDataLen()) {
                readString = data.readKeyString();
                if (readString.equals(goodId)) {
                    long address = data.readLong();
                    RecordIndex result = new RecordIndex((byte)(address&0x3F), address>>6);
                    results.add(result);
                } else {
                    data.skip(8);
                }
            }
            bucketIndex = page.getNextPage();
            if(bucketIndex == -1) {
                break;
            }
        }
        return results;
    }

    private HashMap<String, Object> parserBuffer(Data data, int len, String recordKey) {
        // 格式 [hashCode(int), length(int), keyString(int,string), [key1(int),value(type,[value])], ...]
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put(this.columnsMap[0], recordKey);
        int oldPos = data.getPos();
        while (data.getPos() - oldPos < len) {
            String key = this.columnsMap[data.readInt()];
            byte type = data.readByte();
            Object value;
            switch (type) {
                case Value.BOOLEAN_FALSE:
                    value = false;
                    break;
                case Value.BOOLEAN_TRUE:
                    value = true;
                    break;
                case Value.LONG:
                    value = data.readLong();
                    break;
                case Value.DOUBLE:
                    value = data.readDouble();
                    break;
                default:
                    value = data.readString();
            }
            result.put(key, value);
        }
        return result;
    }

    public HashMap<String, Object> findRecord(String key) {
        int hashCode = HashIndex.getHashCode(key);
        PageStore pageStore = this.storeFiles.get(index.getFileIndex(hashCode));
        int pageId = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(pageId)) {
            return null;
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
                page = pageStore.getPage(pageId);
                data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
                dataLen = page.getDataLen();
                pageId = page.getNextPage();
            }
            if (nextRecord) {
                readHash = data.readInt();
                readLen = data.readInt();
                buffer.reset();
            }
            int copyLen = Math.min(readLen - buffer.getPos(), dataLen - data.getPos());
            if (data.getPos()+copyLen > data.getLength()) {
                throw new RuntimeException("超出数组长度");
            }
            buffer.copyFrom(data, data.getPos(), copyLen);
            data.setPos(data.getPos()+copyLen);
            if (readLen == buffer.getPos()) {
                // 验证
                if (hashCode == readHash) {
                    int bufferLen = buffer.getPos();
                    buffer.reset();
                    String readKey = buffer.readString();
                    if (readKey.equals(key)) {
                        return parserBuffer(buffer, bufferLen - readKey.length() - 4, readKey);
                    }
                }
                nextRecord = true;
            } else {
                nextRecord = false;
            }
        }
        return null;
    }

}
