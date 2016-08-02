package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.IndexEntry;
import com.alibaba.middleware.race.cache.ThreadPool;
import com.alibaba.middleware.race.cache.TwoLevelCache;
import com.alibaba.middleware.race.query.IndexAttachment;
import com.alibaba.middleware.race.query.IndexHandler;
import com.alibaba.middleware.race.store.Data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class BuyerTable {
    private static BuyerTable instance = new BuyerTable();
    public static BuyerTable getInstance() {
        return instance;
    }
    private BuyerTable() { }

    //private TwoLevelCache<String, HashMap<String, String>> resultCache;

    private static final int FIRST_LEVEL_CACHE_SIZE = 8*OrderTable.BASE_SIZE;  // 0.251125k/record
    private static final int SECOND_LEVEL_CACHE_SIZE = 8*OrderTable.BASE_SIZE;

    private static final String[] TABLE_COLUMNS = {"buyerid"};
    public Table baseTable;


    public HashMap<String, Byte> buyerFilesMap;
    public String[] sortBuyerFiles;
    private AsynchronousFileChannel[] fileChannels;

    public ConcurrentHashMap<Long, IndexEntry> indexCache;

    // 在构造之前做初始工作
    public void init(Collection<String> buyerFiles) {
        baseTable = new Table("buyerTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        buyerFilesMap = new HashMap<String, Byte>(buyerFiles.size());
        sortBuyerFiles = new String[buyerFiles.size()];
        for (String file: buyerFiles) {
            byte postfix = (byte)Integer.parseInt(file.substring(file.lastIndexOf('.')+1));
            buyerFilesMap.put(file, postfix);
            sortBuyerFiles[postfix] = file;
        }
        this.indexCache = new ConcurrentHashMap<Long, IndexEntry>(8*1024*OrderTable.BASE_SIZE);
    }

    public void reopen() {
        //resultCache = new TwoLevelCache<String, HashMap<String, String>>(FIRST_LEVEL_CACHE_SIZE, SECOND_LEVEL_CACHE_SIZE);
        this.fileChannels = new AsynchronousFileChannel[this.sortBuyerFiles.length];
        try {
            HashSet<StandardOpenOption>openOptions = new HashSet<StandardOpenOption>(
                    Collections.singleton(StandardOpenOption.READ));
            for (int i = 0; i < sortBuyerFiles.length; i++) {
                this.fileChannels[i] = AsynchronousFileChannel.open(Paths.get(this.sortBuyerFiles[i]),
                        openOptions, ThreadPool.getInstance().pool);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 先从缓存取，取不到再从文件取
//    public HashMap<String, String> find(String buyerId) {
//        HashMap<String, String> result = resultCache.get(buyerId);
//        if (result == null) {
//            result = findFromFile(buyerId);
//            if (result != null) {
//                resultCache.put(buyerId, result);
//            }
//        }
//        return result;
//    }

    // 从缓存中取结果
//    public HashMap<String, String> findFormCache(String buyerId) {
//        return resultCache.get(buyerId);
//    }

    // 同步获取结果
    public HashMap<String, String> findFromFile(String buyerId) {
        HashMap<String, String> result = new HashMap<String, String>();
        Long postfix = Data.getKeyPostfix(buyerId);
        IndexEntry recordIndex = indexCache.get(postfix);
        if (recordIndex == null) {
            return null;
        }
        String fileName = this.sortBuyerFiles[recordIndex.getFileId()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
            long pos = recordIndex.getAddress();
            byte[] buffer = new byte[recordIndex.getLength()];
            randomAccessFile.seek(pos);
            randomAccessFile.readFully(buffer);
            int begin = 0;
            String key="";
            for (int i=0; i<buffer.length; i++) {
                if (buffer[i] == '\n') {
                    result.put(key, new String(buffer, begin, i-begin));
                    break;
                }
                if (buffer[i] == ':') {
                    key = new String(buffer, begin, i-begin);
                    begin = i+1;
                } else if (buffer[i] == '\t') {
                    result.put(key, new String(buffer, begin, i-begin));
                    begin = i+1;
                }
            }
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    // 异步读 使用 CountDownLatch 等待结果 传入的buyerRecord存结果
    public void findBuyer(String buyerId, CountDownLatch waitForBuyer, HashMap<String, String> buyerRecord) {
        IndexHandler indexHandler = new IndexHandler();
        IndexEntry index = indexCache.get(Data.getKeyPostfix(buyerId));
        if (index == null) {
            waitForBuyer.countDown();
            return;
        }
        byte fileId = index.getFileId();
        IndexAttachment attachment = new IndexAttachment(waitForBuyer, buyerRecord, index);
        this.fileChannels[fileId].read(ByteBuffer.wrap(attachment.buffer), index.getAddress(),
                    attachment, indexHandler);
    }

    public HashMap<String, HashMap<String, String>> findBuyerOfOrder(ArrayList<HashMap<String, String>> orderRecords) {
        HashMap<String, HashMap<String, String>> results = new HashMap<String, HashMap<String, String>>();
        HashSet<String> buyerIds = new HashSet<String>();
        for (HashMap<String, String> orderRecord: orderRecords) {
            buyerIds.add(orderRecord.get("buyerid"));
        }
//        HashMap<String, String> buyerRecord;
        ArrayList<IndexEntry> noCache = new ArrayList<IndexEntry>();
        for (String buyerId: buyerIds) {
//            buyerRecord = resultCache.get(buyerId);
//            if (buyerRecord != null) {
//                results.put(buyerId, buyerRecord);
//            } else {
            noCache.add(indexCache.get(Data.getKeyPostfix(buyerId)));
//            }
        }
        try {
            findBuyerRecords(noCache, results);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    private void findBuyerRecords(ArrayList<IndexEntry> indexes, HashMap<String, HashMap<String, String>> results)
            throws IOException, ExecutionException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(indexes.size());
        ArrayList<IndexAttachment> attachments = new ArrayList<IndexAttachment>(indexes.size());
        Collections.sort(indexes);
        IndexHandler indexHandler = new IndexHandler();
        byte fileId;
        for (IndexEntry index:indexes) {
            fileId = index.getFileId();
            if (this.fileChannels[fileId] == null) {
                this.fileChannels[fileId] = AsynchronousFileChannel.open(Paths.get(this.sortBuyerFiles[fileId]),
                        StandardOpenOption.READ);
            }
            IndexAttachment attachment = new IndexAttachment(latch, index);
            attachments.add(attachment);
            this.fileChannels[fileId].read(ByteBuffer.wrap(attachment.buffer), index.getAddress(),
                    attachment, indexHandler);
        }
        latch.await();
//        String buyerId;
        for (IndexAttachment attachment: attachments) {
//            buyerId = attachment.record.get("buyerid");
            results.put(attachment.record.get("buyerid"), attachment.record);
//            resultCache.put(buyerId, attachment.record);
        }
    }
}
