package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.cache.SafeData;
import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.Data;
import com.alibaba.middleware.race.store.HashDataPage;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.type.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HashTable extends Table {
    private HashIndex index;
    private int pageSize;
    private String[] columnsMap;

    public HashTable(String name) {
        this.storeFiles = new ArrayList<PageStore>();
        this.name = name;
    }

    public void init(Collection<String> storeFolders, int bucketSize,
                     int cacheSize, int pageSize) {
        for (String folder: storeFolders) {
            PageStore pageStore = new PageStore(folder + "/" + this.name + ".db",
                    bucketSize, pageSize);
            pageStore.open("rw", cacheSize);
            this.pageSize = pageSize;
            this.storeFiles.add(pageStore);
        }
        this.index = new HashIndex(bucketSize, this.storeFiles.size());
    }

    public void reopen(int cacheSize) {
        for (PageStore pageStore: this.storeFiles) {
            pageStore.open("r", cacheSize);
        }
        this.initColumnsMap();
    }

    private void initColumnsMap() {
        columnsMap = new String[columns.size()];
        for (Map.Entry<String, Column> entry: this.columns.entrySet()) {
            columnsMap[entry.getValue().getColumnId()] = entry.getKey();
        }
    }

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Column>();
        int columnsId = Column.FirstColumnsId;
        for (String key: columnsKeys) {
            Column column = new Column(key, columnsId);
            this.columns.put(key, column);
            columnsId++;
        }
    }

    public HashIndex getIndex() {
        return this.index;
    }

    public ArrayList<BuyerIdRowIndex> findIndex(String buyerId,  long startTime, long endTime) {
        ArrayList<BuyerIdRowIndex> results = new ArrayList<BuyerIdRowIndex>();
        int hashCode = HashIndex.getHashCode(buyerId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketIndex(hashCode);
        int readHashCode;
        String readString;
        long readTime;
        HashDataPage page;
        Data data;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes());
            data.setPos(HashDataPage.HeaderLength);
            // [hashCode(int), buyerId(len,string), createTime(long), fileId(byte), address(long)]
            while (data.getPos() < page.getDataLen()) {
                readHashCode = data.readInt();
                if (readHashCode != hashCode) {
                    data.skip(data.readInt() + 8 + 1 + 8);
                } else {
                    readString = data.readString();
                    if (readString.equals(buyerId)) {
                        readTime = data.readLong();
                        if (readTime >= startTime && readTime < endTime) {
                            BuyerIdRowIndex result = new BuyerIdRowIndex(data.readByte(), data.readLong());
                            result.setBuyerId(buyerId);
                            result.setCreateTime(readTime);
                            results.add(result);
                        } else {
                            data.skip(1+8);
                        }
                    } else {
                        data.skip(8+1+8);
                    }
                }
            }
            bucketIndex = page.getNextPage();
            if(bucketIndex == -1) {
                break;
            }
        }
        return results;
    }

    public OrderIdRowIndex findIndex(long orderId) {
        int hashCode = HashIndex.getHashCode(orderId);
        PageStore pageStore = storeFiles.get(index.getFileIndex(hashCode));
        int bucketIndex = index.getBucketIndex(hashCode);
        HashDataPage page;
        Data data;
        long readOrderId;
        while (true) {
            page = pageStore.getPage(bucketIndex);
            data = new Data(page.getData().getBytes());
            data.setPos(HashDataPage.HeaderLength);
            // [orderId(long), fileId(byte), address(long)]
            while (data.getPos() < page.getDataLen()) {
                readOrderId = data.readLong();
                if (readOrderId != orderId) {
                    data.skip(1+8);
                } else {
                    OrderIdRowIndex result = new OrderIdRowIndex(data.readByte(), data.readLong());
                    result.setOrderId(orderId);
                    return result;
                }
            }
            bucketIndex = page.getNextPage();
            if (bucketIndex == -1) {
                return null;
            }
        }
    }

    public HashMap<String, Object> findOrder(RowIndex rowIndex) {
        PageStore pageStore = this.storeFiles.get(rowIndex.getFileId());
        int pageId = (int)(rowIndex.getAddress()/this.pageSize);
        int offset = (int)(rowIndex.getAddress()%this.pageSize);
        HashDataPage page = pageStore.getPage(pageId);
        int dataLen = page.getDataLen();
        int nextPageId = page.getNextPage();
        Data data = new Data(page.getData().getBytes());
        data.setPos(HashDataPage.HeaderLength);
        data.setPos(offset + 4);   // skip hashCode
        int len = data.readInt();  // 读取数据长度
        if (len < dataLen - data.getPos()) {  // 整个记录在一个页面，直接解析
            return parserBuffer(data, len);
        } else {  // 整个记录在多个页面，复制到缓冲区再解析
            Data buffer = SafeData.getData();
            buffer.reset();
            int leftLen = len;
            while (true) {
                int copyLen = Math.min(leftLen, dataLen-data.getPos());
                System.arraycopy(data.getBytes(), data.getPos(), buffer.getBytes(), buffer.getPos(), copyLen);
                leftLen -= copyLen;
                if (leftLen == 0) {
                    break;
                }
                page = pageStore.getPage(nextPageId);
                data = new Data(page.getData().getBytes());
                data.setPos(HashDataPage.HeaderLength);
                dataLen = page.getDataLen();
            }
            return parserBuffer(buffer, len);
        }
    }

    private HashMap<String, Object> parserBuffer(Data data, int len) {
        String recordKey = data.readString();
        return parserBuffer(data, len-recordKey.length()-4, recordKey);
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
        HashMap<String, Object> result;
        int hashCode = HashIndex.getHashCode(key);
        int pageId = index.getBucketIndex(hashCode);
        int fileId = index.getFileIndex(hashCode);
        PageStore pageStore = this.storeFiles.get(fileId);
        HashDataPage page = pageStore.getPage(pageId);
        Data data = new Data(page.getData().getBytes());
        data.setPos(HashDataPage.HeaderLength);
        pageId = page.getNextPage();
        int dataLen = page.getDataLen();
        while (data.getPos() < dataLen) {
            int readHashCode = data.readInt();
            int readLen = data.readInt();
            if (data.getPos() + readLen > dataLen) {  // 跨页面处理
                if (readHashCode != hashCode) {
                    if (pageId == -1) {
                        return null;
                    }
                    page = pageStore.getPage(pageId);
                    data = new Data(page.getData().getBytes());
                    data.setPos(HashDataPage.HeaderLength);
                    pageId = page.getNextPage();
                    dataLen = page.getDataLen();
                } else {
                    String readKey = data.readString();
                    if (readKey.equals(key)) {  // 找到了记录
                        Data buffer = SafeData.getData();
                        buffer.reset();

                    } else {  // 未找到记录
                        page = pageStore.getPage(pageId);
                        data = new Data(page.getData().getBytes());
                        data.setPos(HashDataPage.HeaderLength);
                        pageId = page.getNextPage();
                        dataLen = page.getDataLen();
                    }
                }
            } else {
                if (readHashCode != hashCode) {
                    data.skip(readLen);
                } else {
                    String readKey = data.readString();
                    if (readKey.equals(key)) {
                        result = parserBuffer(data, readLen-readKey.length()-4, readKey);
                        return result;
                    } else {
                        data.skip(readLen-readKey.length()-4);
                    }
                }
            }
        }
        return null;
    }

    public ArrayList<HashMap<String, Object>> findOrders(String goodId) {
        ArrayList<HashMap<String, Object>> results = new ArrayList<HashMap<String, Object>>();
        int hashCode = HashIndex.getHashCode(goodId);
        int pageId = index.getBucketIndex(hashCode);
        PageStore pageStore = this.storeFiles.get(index.getFileIndex(hashCode));
        HashDataPage page = pageStore.getPage(pageId);
        Data data = new Data(page.getData().getBytes());
        data.setPos(HashDataPage.HeaderLength);
        pageId = page.getNextPage();
        int dataLen = page.getDataLen();
        while (true) {
            int readHashCode = data.readInt();
            int readLen = data.readInt();
            if (data.getPos() + readLen <= dataLen) {  // 记录不跨页 处理记录
                if (readHashCode == hashCode) {  // 解析记录
                    String readKey = data.readString();
                    if (readKey.equals(goodId)) {
                        results.add(parserBuffer(data, readLen-readKey.length()-4, readKey));
                    } else {
                        data.skip(readLen-readKey.length()-4);
                    }
                } else {  // 跳过记录
                    data.skip(readLen);
                }
                if (data.getPos() == dataLen) { // 处理到了页面末尾
                    if (pageId == -1) {  // 没有了更多的页面
                        break;
                    }
                    // 切换到下一页
                    page = pageStore.getPage(pageId);
                    data = new Data(page.getData().getBytes());
                    data.setPos(HashDataPage.HeaderLength);
                    pageId = page.getNextPage();
                    dataLen = page.getDataLen();
                }
            } else {  // 跨页面处理
                int leftLen = readLen + data.getPos() - dataLen;
                if (readHashCode != hashCode) {
                    // 切换到下一页第一条记录
                    page = pageStore.getPage(pageId);
                    data = new Data(page.getData().getBytes());
                    data.setPos(HashDataPage.HeaderLength + leftLen);
                    pageId = page.getNextPage();
                    dataLen = page.getDataLen();
                } else {
                    // 读取完整记录 然后解析
                    Data buffer = SafeData.getData();
                    buffer.reset();
                    buffer.copyFrom(data, data.getPos(), dataLen-data.getPos());
                    while (true) {
                        page = pageStore.getPage(pageId);
                        data = new Data(page.getData().getBytes());
                        data.setPos(HashDataPage.HeaderLength);
                        pageId = page.getNextPage();
                        dataLen = page.getDataLen();
                        if (data.getPos() + leftLen < dataLen) {
                            buffer.copyFrom(data, data.getPos(), leftLen);
                            data.setPos(data.getPos()+leftLen);
                            break;
                        } else {
                            buffer.copyFrom(data, data.getPos(), dataLen-data.getPos());
                            leftLen -= (dataLen-data.getPos());
                        }
                    }
                    buffer.reset();
                    String readKey = buffer.readString();
                    if (readKey.equals(goodId)) {  // 找到了记录
                        results.add(parserBuffer(buffer, buffer.getPos()-readKey.length()-4, readKey));
                    }
                }
            }
        }
        return results;
    }

}
