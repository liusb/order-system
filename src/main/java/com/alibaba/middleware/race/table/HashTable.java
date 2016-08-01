package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.ThreadPool;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.query.BuyerCondition;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.DataPage;
import com.alibaba.middleware.race.store.PageStore;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;


public class HashTable extends Table {
    private HashIndex index;
    protected ArrayList<PageStore> storeFiles;
    private String[] storeFilesName;
    private AsynchronousFileChannel[] fileChannels;

    public HashTable(String name) {
        super(name);
        this.storeFiles = new ArrayList<PageStore>();
    }

    public void init(Collection<String> storeFolders, int bucketSize, int pageSize) {
        String storeFileName;
        storeFilesName = new String[storeFolders.size()];
        int index=0;
        for (String folder: storeFolders) {
            storeFileName = folder + "/" + this.name + ".db";
            PageStore pageStore = new PageStore(storeFileName, bucketSize, pageSize);
            pageStore.open("rw");
            this.storeFiles.add(pageStore);
            storeFilesName[index++] = storeFileName;
        }
        this.index = new HashIndex(bucketSize, this.storeFiles.size());
    }

    public void reopen() {
        for (PageStore pageStore: this.storeFiles) {
            pageStore.open("r");
        }
        fileChannels = new AsynchronousFileChannel[storeFilesName.length];
        try {
            HashSet<StandardOpenOption> openOptions = new HashSet<StandardOpenOption>(
                    Collections.singleton(StandardOpenOption.READ));
            for (int i = 0; i < storeFilesName.length; i++) {
                this.fileChannels[i] = AsynchronousFileChannel.open(Paths.get(this.storeFilesName[i]),
                        openOptions, ThreadPool.getInstance().pool);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public AsynchronousFileChannel getFileChannel(int fileId) {
        return fileChannels[fileId];
    }

    public ArrayList<PageStore> getPageFiles() {
        return this.storeFiles;
    }

    public HashIndex getIndex() {
        return this.index;
    }

    public ArrayList<RecordIndex> findIndex(String buyerId, long startTime, long endTime) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        int hashCode = HashIndex.getHashCode(buyerId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return results;
        }
        short prefix = Data.getKeyPrefix(buyerId);
        short readPrefix;
        long postfix = Data.getKeyPostfix(buyerId);
        long readPostfix;
        long readTime;
        DataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            // [buyerId(string), createTime(long), address(long<<6)|fileId(byte)]
            while (data.getPos() < page.getDataLen()) {
                readPrefix = data.readShort();
                if (readPrefix == prefix) {
                    readPostfix = data.readLong();
                    if (readPostfix == postfix) {
                        readTime = data.readLong();
                        if (readTime >= startTime && readTime < endTime) {
                            long address = data.readLong();
                            RecordIndex result = new RecordIndex((byte) (address & 0x3F), address >> 6);
                            results.add(result);
                        } else {
                            data.skip(8);
                        }
                    } else {
                        data.skip(8+8);
                    }
                } else {
                    data.skip(8+8+8);
                }
            }
            bucketIndex = page.getNextPage();
            if(bucketIndex == -1) {
                break;
            }
        }
        return results;
    }

    public static ArrayList<RecordIndex> findIndex(DataPage page, BuyerCondition condition) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        short readPrefix;
        long readPostfix;
        long readTime;
        Data data = page.getData();
        while (data.getPos() < page.getDataLen()) {
            readPrefix = data.readShort();
            if (readPrefix == condition.prefix) {
                readPostfix = data.readLong();
                if (readPostfix == condition.postfix) {
                    readTime = data.readLong();
                    if (readTime >= condition.startTime && readTime < condition.endTime) {
                        long address = data.readLong();
                        RecordIndex result = new RecordIndex((byte) (address & 0x3F), address >> 6);
                        results.add(result);
                    } else {
                        data.skip(8);
                    }
                } else {
                    data.skip(8+8);
                }
            } else {
                data.skip(8+8+8);
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
        short prefix = Data.getKeyPrefix(goodId);
        short readPrefix;
        long postfix = Data.getKeyPostfix(goodId);
        long readPostfix;
        DataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            // [goodId(string), address(long<<6)|fileId(byte)]
            while (data.getPos() < page.getDataLen()) {
                readPrefix = data.readShort();
                if (readPrefix == prefix) {
                    readPostfix = data.readLong();
                    if (readPostfix == postfix) {
                        long address = data.readLong();
                        RecordIndex result = new RecordIndex((byte) (address & 0x3F), address >> 6);
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

}
