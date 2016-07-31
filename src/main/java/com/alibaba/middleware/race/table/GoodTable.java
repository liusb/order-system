package com.alibaba.middleware.race.table;


import com.alibaba.middleware.race.cache.IndexCache;
import com.alibaba.middleware.race.cache.IndexEntry;
import com.alibaba.middleware.race.cache.TwoLevelCache;
import com.alibaba.middleware.race.store.Data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Collection;
import java.util.HashMap;

public class GoodTable {
    private static GoodTable instance = new GoodTable();
    public static GoodTable getInstance() {
        return instance;
    }
    private GoodTable() { }

    private TwoLevelCache<String, HashMap<String, String>> resultCache;

    private static final int TABLE_BUCKET_SIZE = 2;
    private static final int FIRST_LEVEL_CACHE_SIZE = 3*256*OrderTable.BASE_SIZE;  //0.904k/record
    private static final int SECOND_LEVEL_CACHE_SIZE = 256*OrderTable.BASE_SIZE;
    
    // 每页的大小，单位为byte
    private static final int TABLE_PAGE_SIZE = 4*(1<<10);
    private static final String[] TABLE_COLUMNS = {"goodid"};
    public HashTable baseTable;

    public HashMap<String, Byte> goodFilesMap;
    public String[] sortGoodFiles;
    private AsynchronousFileChannel[] fileChannels;

    public IndexCache indexCache;

    // 在构造之前做初始工作
    public void init(Collection<String> storeFolders, Collection<String> goodFiles) {
        baseTable = new HashTable("goodTable");
        baseTable.setBaseColumns(TABLE_COLUMNS);
        baseTable.init(storeFolders, TABLE_BUCKET_SIZE, TABLE_PAGE_SIZE);
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
        this.baseTable.reopen();
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
        String fileName = this.sortGoodFiles[recordIndex.getFileId()];
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
}
