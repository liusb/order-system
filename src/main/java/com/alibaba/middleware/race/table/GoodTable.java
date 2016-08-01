package com.alibaba.middleware.race.table;


import com.alibaba.middleware.race.cache.IndexCache;
import com.alibaba.middleware.race.cache.IndexEntry;
import com.alibaba.middleware.race.cache.TwoLevelCache;
import com.alibaba.middleware.race.index.RecordIndex;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class GoodTable {
    private static GoodTable instance = new GoodTable();
    public static GoodTable getInstance() {
        return instance;
    }
    private GoodTable() { }

    private TwoLevelCache<String, HashMap<String, String>> resultCache;

    private static final int FIRST_LEVEL_CACHE_SIZE = 2*256*OrderTable.BASE_SIZE;  //0.904k/record
    private static final int SECOND_LEVEL_CACHE_SIZE = 2*256*OrderTable.BASE_SIZE;
    
    private static final String[] TABLE_COLUMNS = {"goodid"};
    public Table baseTable;

    public HashMap<String, Byte> goodFilesMap;
    public String[] sortGoodFiles;
    private AsynchronousFileChannel[] fileChannels;

    public IndexCache indexCache;

    // 在构造之前做初始工作
    public void init(Collection<String> goodFiles) {
        baseTable = new HashTable("goodTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        goodFilesMap = new HashMap<String, Byte>(5);
        sortGoodFiles = new String[5];
        for (String file: goodFiles) {
            byte postfix = (byte)Integer.parseInt(file.substring(file.lastIndexOf('.')+1));
            goodFilesMap.put(file, postfix);
            sortGoodFiles[postfix] = file;
        }
        this.indexCache = new IndexCache(4*1024*OrderTable.BASE_SIZE);
    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {
        resultCache = new TwoLevelCache<String, HashMap<String, String>>(FIRST_LEVEL_CACHE_SIZE, SECOND_LEVEL_CACHE_SIZE);
        this.fileChannels = new AsynchronousFileChannel[this.sortGoodFiles.length];
    }

    public HashMap<String, String> find(String goodId) {
        HashMap<String, String> result = resultCache.get(goodId);
        if (result == null) {
            result = findFromFile(goodId);
            if (result != null) {
                resultCache.put(goodId, result);
            }
        } else {
//            System.out.println("命中缓存 goodId: " + goodId);
        }
        return result;
    }

    public HashMap<String, String> findFromFile(String buyerId) {
        HashMap<String, String> result = new HashMap<String, String>();
        Long postfix = Data.getKeyPostfix(buyerId);
        short prefix = Data.getKeyPrefix(buyerId);
        IndexEntry recordIndex = indexCache.get(postfix, prefix);
        if (recordIndex == null) {
            return null;
        }
        String fileName = this.sortGoodFiles[recordIndex.getFileId()];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
            long fileSize = randomAccessFile.length();
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

    public HashMap<String, HashMap<String, String>> findGoodOfOrder(ArrayList<HashMap<String, String>> orderRecords) {
        HashMap<String, HashMap<String, String>> results = new HashMap<String, HashMap<String, String>>();
        HashSet<String> goodIds = new HashSet<String>();
        for (HashMap<String, String> orderRecord: orderRecords) {
            goodIds.add(orderRecord.get("goodid"));
        }
        HashMap<String, String> goodRecord;
        ArrayList<IndexEntry> noCache = new ArrayList<IndexEntry>();
        for (String goodId: goodIds) {
            goodRecord = resultCache.get(goodId);
            if (goodRecord != null) {
                results.put(goodId, goodRecord);
            } else {
                noCache.add(indexCache.get(Data.getKeyPostfix(goodId), Data.getKeyPrefix(goodId)));
            }
        }
        try {
            findGoodRecords(noCache, results);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    private void findGoodRecords(ArrayList<IndexEntry> indexes, HashMap<String, HashMap<String, String>> results)
            throws IOException, ExecutionException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(indexes.size());
        ArrayList<IndexAttachment> attachments = new ArrayList<IndexAttachment>(indexes.size());
        Collections.sort(indexes);
        IndexHandler indexHandler = new IndexHandler();
        byte fileId;
        for (IndexEntry index:indexes) {
            fileId = index.getFileId();
            if (this.fileChannels[fileId] == null) {
                this.fileChannels[fileId] = AsynchronousFileChannel.open(Paths.get(this.sortGoodFiles[fileId]),
                        StandardOpenOption.READ);
            }
            IndexAttachment attachment = new IndexAttachment(latch, index);
            attachments.add(attachment);
            this.fileChannels[fileId].read(ByteBuffer.wrap(attachment.buffer), index.getAddress(),
                    attachment, indexHandler);
        }
        latch.await();
        String goodId;
        for (IndexAttachment attachment: attachments) {
            goodId = attachment.record.get("goodid");
            results.put(attachment.record.get("goodid"), attachment.record);
            resultCache.put(goodId, attachment.record);
        }
    }

}
