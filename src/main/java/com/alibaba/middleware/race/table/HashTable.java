package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.Page;
import com.alibaba.middleware.race.store.PageStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class HashTable extends Table {
    private HashIndex index;
    private int pageSize;

    public HashTable(String name) {
        this.storeFiles = new ArrayList<PageStore>();
        this.name = name;
    }

    public void init(Collection<String> storeFolders, int bucketSize,
                     int cacheSize, int pageSize) {
        for (String folder: storeFolders) {
            PageStore pageStore = new PageStore(folder + "/" + this.name + ".db",
                    bucketSize, pageSize);
            pageStore.open("rm", cacheSize);
            this.pageSize = pageSize;
            this.storeFiles.add(pageStore);
        }
        this.index = new HashIndex(bucketSize, this.storeFiles.size());
    }

    public void reopen(int cacheSize) {
        for (PageStore pageStore: this.storeFiles) {
            pageStore.open("r", cacheSize);
        }
    }

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Column>();
        int columnsId = 0;
        for (String key: columnsKeys) {
            Column column = new Column(key, columnsId);
            this.columns.put(key, column);
            columnsId++;
        }
    }

    public void setBaseColumns(HashMap<String, Column> baseColumns) {
        this.columns = baseColumns;
    }

    public HashIndex getIndex() {
        return this.index;
    }

    public ArrayList<BuyerIdRowIndex> findIndex(String buyerId,  long startTime, long endTime) {
        int hashCode = HashIndex.getHashCode(buyerId);
        int fileIndex = index.getFileIndex(hashCode);
        int bucketIndex = index.getBucketIndex(hashCode);
        storeFiles.get(fileIndex);

        return null;
    }

    public OrderIdRowIndex findIndex(long orderId) {
        int hashCode = HashIndex.getHashCode(orderId);
        int fileIndex = index.getFileIndex(hashCode);
        int bucketIndex = index.getBucketIndex(hashCode);
        Data data = storeFiles.get(fileIndex).getPage(bucketIndex).getData();
        int length = data.getLength();
        return null;
    }

    public HashMap<String, Object> findOrder(RowIndex rowIndex) {
        PageStore pageStore = this.storeFiles.get(rowIndex.getFileId());
        int pageId = (int)(rowIndex.getAddress()/this.pageSize);
        int offset = (int)(rowIndex.getAddress()%this.pageSize);
        Page page = pageStore.getPage(pageId);
        Data data = page.getData();
        data.setPos(offset+4);
        int len = data.readInt();

        return null;
    }

    public HashMap<String, Object> findBuyer(String buyerId) {
        int hashCode = HashIndex.getHashCode(buyerId);
        int pageId = index.getBucketIndex(hashCode);
        int fileId = index.getFileIndex(hashCode);
        Page page = storeFiles.get(fileId).getPage(pageId);
        Data data = page.getData();
        int len = data.readInt();

        return null;
    }

    public HashMap<String, Object> findGood(String goodId) {
        int hashCode = HashIndex.getHashCode(goodId);
        int pageId = index.getBucketIndex(hashCode);
        int fileId = index.getFileIndex(hashCode);
        Page page = storeFiles.get(fileId).getPage(pageId);
        Data data = page.getData();
        int len = data.readInt();

        return null;
    }
}
