package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.TwoLevelCache;
import com.alibaba.middleware.race.index.RecordIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderTable {
    private static OrderTable instance = new OrderTable();
    public static OrderTable getInstance() {
        return instance;
    }
    private OrderTable() {}

    private TwoLevelCache<Long, HashMap<String, String>> resultCache;

    public static final int BASE_SIZE = 1024;
    private static final int GOOD_INDEX_BUCKET_SIZE = 64*BASE_SIZE;
    private static final int ORDER_INDEX_BUCKET_SIZE = 64*BASE_SIZE;
    private static final int BUYER_INDEX_BUCKET_SIZE = 128*BASE_SIZE;
    private static final int FIRST_LEVEL_CACHE_SIZE = 7*1024*BASE_SIZE;  // 0.21676k/record
    private static final int SECOND_LEVEL_CACHE_SIZE = 2*1024*BASE_SIZE;

    // 每页的大小，单位为byte
    private static final int GOOD_TABLE_PAGE_SIZE = 4*(1<<10);
    // 存储索引为goodId, 存储格式为[goodId, orderId, buyerId, createTime, ......]
    public HashTable goodIndex;

    private static final String[] INDEX_COLUMNS = {};   // 索引不需要列信息
    private static final int ORDER_INDEX_PAGE_SIZE = 4*(1<<10);  // 1KB
    // 索引为orderId 记录存储格式为[orderId, fileId, address](long, byte, long)
    public HashTable orderIndex;

    private static final int BUYER_INDEX_PAGE_SIZE = 4*(1<<10);
    // 索引为buyerId, 记录存储格式为[createTime, buyerId, fileId, address](long, string, byte, long)
    public HashTable buyerIndex;

    public HashMap<String, Byte> orderFilesMap;
    private String[] sortOrderFiles;

    public void init(Collection<String> storeFolders, Collection<String> orderFiles) {
        goodIndex = new HashTable("orderTable");
        goodIndex.setBaseColumns(INDEX_COLUMNS);
        goodIndex.init(storeFolders, GOOD_INDEX_BUCKET_SIZE, GOOD_TABLE_PAGE_SIZE);

        orderIndex = new HashTable("orderIndex");
        orderIndex.setBaseColumns(INDEX_COLUMNS);
        orderIndex.init(storeFolders, ORDER_INDEX_BUCKET_SIZE, ORDER_INDEX_PAGE_SIZE);

        buyerIndex = new HashTable("buyerIndex");
        buyerIndex.setBaseColumns(INDEX_COLUMNS);
        buyerIndex.init(storeFolders, BUYER_INDEX_BUCKET_SIZE, BUYER_INDEX_PAGE_SIZE);

        orderFilesMap = new HashMap<String, Byte>(43);
        for (String file: orderFiles) {
            byte postfix = (byte)Integer.parseInt(file.substring(file.lastIndexOf('.')+1));
            orderFilesMap.put(file, postfix);
        }
        sortOrderFiles = new String[43];
        for (String file: orderFiles) {
            int postfix = Integer.parseInt(file.substring(file.lastIndexOf('.')+1));
            sortOrderFiles[postfix] = file;
        }
    }

    private volatile boolean prepared = false;

    public boolean isPrepared() {
        return prepared;
    }

    public void reopen() {
        goodIndex.reopen();
        orderIndex.reopen();
        buyerIndex.reopen();
        this.prepared = true;
        resultCache = new TwoLevelCache<Long, HashMap<String, String>>(FIRST_LEVEL_CACHE_SIZE, SECOND_LEVEL_CACHE_SIZE);
    }

    public RecordIndex findOderIdIndex(long orderId) {
        return this.orderIndex.findIndex(orderId);
    }

    public ArrayList<RecordIndex> findGoodIdIndex(String goodId) {
        return this.goodIndex.findIndex(goodId);
    }

    public ArrayList<RecordIndex> findBuyerIdIndex(String buyerId, long startTime, long endTime) {
        return this.buyerIndex.findIndex(buyerId, startTime, endTime);
    }

    public HashMap<String, String> findOrderRecord(RecordIndex recordIndex) {
        HashMap<String, String> result = new HashMap<String, String>();
        String fileName = this.sortOrderFiles[recordIndex.getFileId()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
            long fileSize = randomAccessFile.length();
            long pos = recordIndex.getAddress();
            byte[] buffer = new byte[(int)Math.min(2048, fileSize-pos)];
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

    public HashMap<String, String> findOrder(RecordIndex recordIndex) {
        long cacheIndex = (recordIndex.getAddress()<<6)|recordIndex.getFileId();
        HashMap<String, String> result = resultCache.get(cacheIndex);
        if (result == null) {
            result = this.findOrderRecord(recordIndex);
            if (result != null) {
                resultCache.put(cacheIndex, result);
            }
        } else {
//            System.out.println("命中缓存 orderId:" + result.get("orderid") + " fileId:"
//                    + recordIndex.getFileId() + " address:" + recordIndex.getAddress());
        }
        return result;
    }

    public ArrayList<HashMap<String, String>> findOrders(ArrayList<RecordIndex> recordIndices) {
        ArrayList<HashMap<String, String>> results = new ArrayList<HashMap<String, String>>();
        ArrayList<RecordIndex> notCache = new ArrayList<RecordIndex>();
        HashMap<String, String> result;
        long cacheIndex;
        for (RecordIndex recordIndex: recordIndices) {
            cacheIndex = (recordIndex.getAddress()<<6)|recordIndex.getFileId();
            result = resultCache.get(cacheIndex);
            if (result != null) {
                results.add(result);
            } else {
                notCache.add(recordIndex);
            }
        }
        try {
            aioFindOrderRecord(notCache, results);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return results;
    }

    class AioResult {
        Future<Integer> future;
        byte[] buffer;
        RecordIndex index;
        AioResult(Future<Integer> future, byte[] buffer, RecordIndex index) {
            this.future = future;
            this.buffer = buffer;
            this.index = index;
        }
    }

    private void aioFindOrderRecord(ArrayList<RecordIndex> recordIndexes, ArrayList<HashMap<String, String>> results)
            throws IOException, ExecutionException, InterruptedException {
        ArrayList<AioResult> aioResults = new ArrayList<AioResult>(recordIndexes.size());
        Collections.sort(recordIndexes);
        AsynchronousFileChannel[] channels = new AsynchronousFileChannel[43];
        byte fileId;
        for (RecordIndex recordIndex:recordIndexes) {
            fileId = recordIndex.getFileId();
            if (channels[fileId] == null) {
                channels[fileId] = AsynchronousFileChannel.open(Paths.get(this.sortOrderFiles[fileId]),
                        StandardOpenOption.READ);
            }
            byte[] buffer = new byte[600];
            aioResults.add(new AioResult(channels[fileId].read(ByteBuffer.wrap(buffer), recordIndex.getAddress()),
                    buffer, recordIndex));
        }
        while (!aioResults.isEmpty()) {
            Iterator<AioResult> iterator = aioResults.iterator();
            while (iterator.hasNext()) {
                AioResult aioResult = iterator.next();
                if (aioResult.future.isDone()) {
                    int readLen = aioResult.future.get();
                    HashMap<String, String> result = new HashMap<String, String>();
                    byte[] buffer = aioResult.buffer;
                    int begin = 0;
                    String key = "";
                    for (int i = 0; i < readLen; i++) {
                        if (buffer[i] == '\n') {
                            result.put(key, new String(buffer, begin, i - begin));
                            break;
                        }
                        if (buffer[i] == ':') {
                            key = new String(buffer, begin, i - begin);
                            begin = i + 1;
                        } else if (buffer[i] == '\t') {
                            result.put(key, new String(buffer, begin, i - begin));
                            begin = i + 1;
                        }
                    }
                    results.add(result);
                    resultCache.put((aioResult.index.getAddress()<<6)|aioResult.index.getFileId(), result);
                    iterator.remove();
                }
            }
        }
        for (AsynchronousFileChannel channel: channels) {
            if (channel != null) {
                channel.close();
            }
        }
    }
}
