package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.IndexCache;
import com.alibaba.middleware.race.cache.IndexEntry;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class BuyerTable {
    private static BuyerTable instance = new BuyerTable();
    public static BuyerTable getInstance() {
        return instance;
    }
    private BuyerTable() { }

    private TwoLevelCache<String, HashMap<String, String>> resultCache;

    private static final int FIRST_LEVEL_CACHE_SIZE = 3*1024*OrderTable.BASE_SIZE;  // 0.251125k/record
    private static final int SECOND_LEVEL_CACHE_SIZE = 1024*OrderTable.BASE_SIZE;

    private static final String[] TABLE_COLUMNS = {"buyerid"};
    public Table baseTable;


    public HashMap<String, Byte> buyerFilesMap;
    public String[] sortBuyerFiles;
    private AsynchronousFileChannel[] fileChannels;

    public IndexCache indexCache;

    // 在构造之前做初始工作
    public void init(Collection<String> buyerFiles) {
        baseTable = new Table("buyerTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        buyerFilesMap = new HashMap<String, Byte>(5);
        sortBuyerFiles = new String[5];
        for (String file: buyerFiles) {
            byte postfix = (byte)Integer.parseInt(file.substring(file.lastIndexOf('.')+1));
            buyerFilesMap.put(file, postfix);
            sortBuyerFiles[postfix] = file;
        }
        this.indexCache = new IndexCache(8*1024*OrderTable.BASE_SIZE);
    }

    // 在构造完，准备查询前重新打开，以只读方式打开，缓存为只读，
    public void reopen() {
        resultCache = new TwoLevelCache<String, HashMap<String, String>>(FIRST_LEVEL_CACHE_SIZE, SECOND_LEVEL_CACHE_SIZE);
        this.fileChannels = new AsynchronousFileChannel[this.sortBuyerFiles.length];
    }


    public HashMap<String, String> find(String buyerId) {
        HashMap<String, String> result = resultCache.get(buyerId);
        if (result == null) {
            result = findFromFile(buyerId);
            if (result != null) {
                resultCache.put(buyerId, result);
            }
        } else {
//            System.out.println("命中缓存 buyerId: " + buyerId);
        }
        return result;
    }

    public HashMap<String, String> getFormCache(String buyerId) {
        return resultCache.get(buyerId);
    }

    public HashMap<String, String> findFromFile(String buyerId) {
        HashMap<String, String> result = new HashMap<String, String>();
        Long postfix = Data.getKeyPostfix(buyerId);
        short prefix = Data.getKeyPrefix(buyerId);
        IndexEntry recordIndex = indexCache.get(postfix, prefix);
        if (recordIndex == null) {
            return null;
        }
        String fileName = this.sortBuyerFiles[recordIndex.getFileId()];
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

    public void findBuyer(String buyerId, CountDownLatch waitForBuyer, HashMap<String, String> buyerRecord) throws IOException {
        IndexHandler indexHandler = new IndexHandler();
        IndexEntry index = indexCache.get(Data.getKeyPostfix(buyerId), Data.getKeyPrefix(buyerId));
        if (index == null) {
            waitForBuyer.countDown();
            return;
        }
        byte fileId = index.getFileId();
        if (this.fileChannels[fileId] == null) {
            this.fileChannels[fileId] = AsynchronousFileChannel.open(Paths.get(this.sortBuyerFiles[fileId]),
                    StandardOpenOption.READ);
        }
        IndexAttachment attachment = new IndexAttachment(waitForBuyer, buyerRecord, index);
        this.fileChannels[fileId].read(ByteBuffer.wrap(attachment.buffer), index.getAddress(),
                    attachment, indexHandler);
    }

}
