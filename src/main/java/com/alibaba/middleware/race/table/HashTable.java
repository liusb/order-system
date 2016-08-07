package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.ThreadPool;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.query.BuyerCondition;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.DataPage;
import com.alibaba.middleware.race.store.PageStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Future;


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

    public static ArrayList<RecordIndex> findIndex(DataPage page, BuyerCondition condition) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        long readPostfix;
        long readTime;
        Data data = page.getData();
        while (data.getPos() < page.getDataLen()) {
                readPostfix = data.readLong();
                if (readPostfix == condition.postfix) {
                    readTime = data.readLong();
                    if (readTime >= condition.startTime && readTime < condition.endTime) {
                        RecordIndex result = new RecordIndex(data.readByte(), data.readInt());
                        results.add(result);
                    } else {
                        data.skip(1+4);
                    }
                } else {
                    data.skip(8+1+4);
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
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            while (data.getPos() < page.getDataLen()) {
                readOrderId = data.readLong();
                if (readOrderId != orderId) {
                    data.skip(1+4);
                } else {
                    return new RecordIndex(data.readByte(), data.readInt());
                }
            }
            bucketIndex = page.getNextPage();
            if (bucketIndex == -1) {
                return null;
            }
        }
    }

    public RecordIndex findIndexAio(long orderId) {
        int hashCode = HashIndex.getHashCode(orderId);
        int fileId = index.getFileIndex(hashCode);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return null;
        }
        Data data;
        int dataLen;
        byte[] buffer = new byte[4*1024];
        Future<Integer> readFuture = fileChannels[fileId].read(ByteBuffer.wrap(buffer), ((long) bucketIndex) * 4 * 1024);
        long readOrderId;
        try {
            while (true) {
                readFuture.get();
                data = new Data(buffer);
                dataLen = data.readInt();
                bucketIndex = data.readInt();
                if (bucketIndex != -1) {
                    buffer = new byte[4 * 1024];
                    readFuture = fileChannels[fileId].read(ByteBuffer.wrap(buffer), ((long) bucketIndex) * 4 * 1024);
                }
                while (data.getPos() < dataLen) {
                    readOrderId = data.readLong();
                    if (readOrderId != orderId) {
                        data.skip(1 + 4);
                    } else {
                        return new RecordIndex(data.readByte(), data.readInt());
                    }
                }
                if (bucketIndex == -1) {
                    return null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
        long postfix = Data.getKeyPostfix(goodId);
        long readPostfix;
        DataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes(), DataPage.HeaderLength);
            while (data.getPos() < page.getDataLen()) {
                readPostfix = data.readLong();
                if (readPostfix == postfix) {
                    RecordIndex result = new RecordIndex(data.readByte(), data.readInt());
                    results.add(result);
                } else {
                    data.skip(1+4);
                }
            }
            bucketIndex = page.getNextPage();
            if(bucketIndex == -1) {
                break;
            }
        }
        return results;
    }

    public ArrayList<RecordIndex> findIndexAio(String goodId) {
        ArrayList<RecordIndex> results = new ArrayList<RecordIndex>();
        int hashCode = HashIndex.getHashCode(goodId);
        int fileId = index.getFileIndex(hashCode);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketId(hashCode);
        if (!pageStore.isBucketUsed(bucketIndex)) {
            return results;
        }
        long postfix = Data.getKeyPostfix(goodId);
        long readPostfix;
        Data data;
        int dataLen;
        byte[] buffer = new byte[4*1024];
        Future<Integer> readFuture = fileChannels[fileId].read(ByteBuffer.wrap(buffer), ((long) bucketIndex) * 4 * 1024);
        try {
            while (true) {
                readFuture.get();
                data = new Data(buffer);
                dataLen = data.readInt();
                bucketIndex = data.readInt();
                if (bucketIndex != -1) {
                    buffer = new byte[4 * 1024];
                    readFuture = fileChannels[fileId].read(ByteBuffer.wrap(buffer), ((long) bucketIndex) * 4 * 1024);
                }
                while (data.getPos() < dataLen) {
                    readPostfix = data.readLong();
                    if (readPostfix == postfix) {
                        RecordIndex result = new RecordIndex(data.readByte(), data.readInt());
                        results.add(result);
                    } else {
                        data.skip(1 + 4);
                    }
                }
                if (bucketIndex == -1) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }
}
