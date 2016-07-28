package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.RecordIndex;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class OrderTable {
    private static OrderTable instance = new OrderTable();
    public static OrderTable getInstance() {
        return instance;
    }
    private OrderTable() {}


    private static final int GOOD_INDEX_BUCKET_SIZE = 64;
    private static final int ORDER_INDEX_BUCKET_SIZE = 64;
    private static final int BUYER_INDEX_BUCKET_SIZE = 64;

//    private static final int GOOD_INDEX_BUCKET_SIZE = 64*(1<<10);
//    private static final int ORDER_INDEX_BUCKET_SIZE = 64*(1<<10);
//    private static final int BUYER_INDEX_BUCKET_SIZE = 128*(1<<10);

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

    public void reopen() {
        goodIndex.reopen();
        orderIndex.reopen();
        buyerIndex.reopen();
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

    public HashMap<String, String> findOrder(RecordIndex recordIndex) {
        HashMap<String, String> result = new HashMap<String, String>();
        String fileName = this.sortOrderFiles[recordIndex.getFileId()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
            long fileSize = randomAccessFile.length();
            long pos = recordIndex.getAddress();
            byte[] buffer = new byte[Math.min(2048, (int)(fileSize-pos))];
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

    public ArrayList<HashMap<String, String>> findOrders(ArrayList<RecordIndex> recordIndices) {
        ArrayList<HashMap<String, String>> results = new ArrayList<HashMap<String, String>>();
        for (RecordIndex index: recordIndices) {
            results.add(findOrder(index));
        }
        return results;
    }
}
