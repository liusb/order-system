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
        for (Map.Entry<String, Integer> entry: this.columns.entrySet()) {
            columnsMap[entry.getValue()] = entry.getKey();
        }
    }

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Integer>();
        int columnsId = columns.size();
        for (String key: columnsKeys) {
            this.columns.put(key, columnsId);
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
        int bucketIndex = index.getBucketId(hashCode);
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
        int bucketIndex = index.getBucketId(hashCode);
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
        while (data.getPos() - oldPos != len) {  // todo 如果条件一直不成立，则说明有bug
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
        int hashCode = HashIndex.getHashCode(key);
        PageStore pageStore = this.storeFiles.get(index.getFileIndex(hashCode));
        int pageId = index.getBucketId(hashCode);
        int readHash=0;
        int readLen=0;
        int dataLen = 0;
        HashDataPage page;
        Data data = new Data(new byte[1]);
        Data buffer = SafeData.getData();
        boolean nextRecord = true;
        while (true) {
            if (dataLen == data.getPos()) {
                if(pageId == -1) {
                    break;
                }
                page = pageStore.getPage(pageId);
                data = new Data(page.getData().getBytes(), HashDataPage.HeaderLength);
                dataLen = page.getDataLen();
                pageId = page.getNextPage();
            }
            if (nextRecord) {
                readHash = data.readInt();
                readLen = data.readInt();
                buffer.reset();
            }
            int copyLen = Math.min(readLen - buffer.getPos(), dataLen - data.getPos());
            if (data.getPos()+copyLen > data.getLength()) {
                throw new RuntimeException("超出数组长度");
            }
            buffer.copyFrom(data, data.getPos(), copyLen);
            data.setPos(data.getPos()+copyLen);
            if (readLen == buffer.getPos()) {
                // 验证
                if (hashCode == readHash) {
                    int bufferLen = buffer.getPos();
                    buffer.reset();
                    String readKey = buffer.readString();
                    if (readKey.equals(key)) {
                        return parserBuffer(buffer, bufferLen - readKey.length() - 4, readKey);
                    }
                }
                nextRecord = true;
            } else {
                nextRecord = false;
            }
        }
        return null;
    }


//    public HashMap<String, Object> findRecord(String key) {
//        int hashCode = HashIndex.getHashCode(key);
//        PageStore pageStore = this.storeFiles.get(index.getFileIndex(hashCode));
//        HashDataPage page = pageStore.getPage(index.getBucketId(hashCode));
//        Data data = new Data(page.getData().getBytes(), HashDataPage.HeaderLength);
//        int readHashCode = 0;
//        int needProcessLen = 0;
//        Data buffer = SafeData.getData();
//        while (true) {
//            if (page.getDataLen() == data.getPos()) {   //已经处理到末尾 需要切换到下一页
//                if (page.getNextPage() == -1) {  // 没有了更多的页面
//                    break;
//                }
//                page = pageStore.getPage(page.getPageId());  // 切换到下一页
//                data = new Data(page.getData().getBytes(), HashDataPage.HeaderLength);
//            }
//            if (needProcessLen == 0) {  // 已经处理完一条记录，需要处理下一条记录
//                int oldHash = readHashCode;
//                readHashCode = data.readInt();
//                needProcessLen = data.readInt();
//                if (needProcessLen > 1280000) {
//                    throw new RuntimeException("完全不合理");
//                }
//                buffer.reset();
//            }
//            if (readHashCode == 687769234) {
//                System.out.println();
//            }
//            int processLen = Math.min(needProcessLen, page.getDataLen() - data.getPos());
//            needProcessLen -= processLen;
//            if (hashCode == readHashCode) {  // 处理记录
////                if (buffer.getPos()==0 && needProcessLen==0) {  // 可以直接处理
////                    String readKey = data.readString();
////                    if (readKey.equals(key)) {
////                        return  parserBuffer(data, processLen-readKey.length()-4, readKey);
////                    } else {
////                        data.skip(processLen-readKey.length()-4);
////                    }
////                } else {  // 复制到buffer中
//                buffer.copyFrom(data, data.getPos(), processLen);
//                data.skip(processLen);
//                if (needProcessLen == 0) {   // 全部复制完毕
//                    int bufferLen = buffer.getPos();
//                    buffer.reset();
//                    String readKey = buffer.readString();
//                    if (readKey.equals(key)) {
//                        return parserBuffer(buffer, bufferLen - readKey.length() - 4, readKey);
//                    }
//                }
//                //}
//            } else {  // 直接跳过
//                data.skip(processLen);
//            }
//        }
//        return null;
//    }


//    public HashMap<String, Object> findRecord(String key) {
//        int hashCode = HashIndex.getHashCode(key);
//        int pageId = index.getBucketId(hashCode);
//        PageStore pageStore = this.storeFiles.get(index.getFileIndex(hashCode));
//        HashDataPage page = pageStore.getPage(pageId);
//        Data data = new Data(page.getData().getBytes());
//        data.setPos(HashDataPage.HeaderLength);
//        pageId = page.getNextPage();
//        int dataLen = page.getDataLen();
//        while (true) {
//            int readHashCode = data.readInt();
//            int readLen = data.readInt();
//            if (data.getPos() + readLen <= dataLen) {  // 记录不跨页 处理记录
//                if (readHashCode == hashCode) {  // 解析记录
//                    String readKey = data.readString();
//                    if (readKey.equals(key)) {
//                        return parserBuffer(data, readLen-readKey.length()-4, readKey);
//                    } else {
//                        data.skip(readLen-readKey.length()-4);
//                    }
//                } else {  // 跳过记录
//                    data.skip(readLen);
//                }
//                if (data.getPos() == dataLen) { // 处理到了页面末尾
//                    if (pageId == -1) {  // 没有了更多的页面
//                        break;
//                    }
//                    // 切换到下一页
//                    page = pageStore.getPage(pageId);
//                    data = new Data(page.getData().getBytes());
//                    data.setPos(HashDataPage.HeaderLength);
//                    pageId = page.getNextPage();
//                    dataLen = page.getDataLen();
//                }
//            } else {  // 跨页面处理
//                int leftLen = readLen - (dataLen - data.getPos());  // 剩余长度
//                if (readHashCode != hashCode) {
//                    // 切换到下一条记录
//                    while (true) {
//                        page = pageStore.getPage(pageId);
//                        data = new Data(page.getData().getBytes(), HashDataPage.HeaderLength);
//                        pageId = page.getNextPage();
//                        dataLen = page.getDataLen();
//                        if (data.getPos() + leftLen < dataLen) {
//                            data.skip(leftLen);
//                            break;
//                        } else {
//                            leftLen -= (dataLen-HashDataPage.HeaderLength);
//                            if (leftLen == 0 && pageId == -1) {   // 跳出循环
//                                break;
//                            }
//                        }
//                    }
//                } else {
//                    // 读取完整记录 然后解析
//                    Data buffer = SafeData.getData();
//                    buffer.reset();
//                    buffer.copyFrom(data, data.getPos(), dataLen-data.getPos());
//                    while (true) {   // 还剩leftLen 需要拷贝
//                        page = pageStore.getPage(pageId);
//                        data = new Data(page.getData().getBytes());
//                        data.setPos(HashDataPage.HeaderLength);
//                        pageId = page.getNextPage();
//                        dataLen = page.getDataLen();
//                        if (data.getPos() + leftLen < dataLen) {
//                            buffer.copyFrom(data, data.getPos(), leftLen);
//                            data.setPos(data.getPos()+leftLen);
//                            break;
//                        } else {
//                            buffer.copyFrom(data, data.getPos(), dataLen-data.getPos());
//                            leftLen -= (dataLen-data.getPos());
//                            if (leftLen == 0 && pageId == -1) {
//                                break;
//                            }
//                        }
//                    }
//                    int bufferLen = buffer.getPos();
//                    buffer.reset();
//                    String readKey = buffer.readString();
//                    if (readKey.equals(key)) {  // 找到了记录
//                        return parserBuffer(buffer, bufferLen-readKey.length()-4, readKey);
//                    }
//                }
//                if (pageId == -1) {   // 跳出循环
//                    break;
//                }
//            }
//        }
//        return null;
//    }

    public ArrayList<HashMap<String, Object>> findOrders(String goodId) {
        ArrayList<HashMap<String, Object>> results = new ArrayList<HashMap<String, Object>>();
        int hashCode = HashIndex.getHashCode(goodId);
        int pageId = index.getBucketId(hashCode);
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
                int leftLen = readLen - (dataLen - data.getPos());  // 剩余长度
                if (readHashCode != hashCode) {
                    // 切换到下一条记录
                    while (true) {
                        page = pageStore.getPage(pageId);
                        data = new Data(page.getData().getBytes(), HashDataPage.HeaderLength);
                        pageId = page.getNextPage();
                        dataLen = page.getDataLen();
                        if (data.getPos() + leftLen < dataLen) {
                            data.skip(leftLen);
                            break;
                        } else {
                            leftLen -= (dataLen-HashDataPage.HeaderLength);
                            if (leftLen == 0 && pageId == -1) {   // 跳出循环
                                break;
                            }
                        }
                    }
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
                            if (leftLen == 0 && pageId == -1) {   // 正好整页面读取完毕
                                break;
                            }
                        }
                    }
                    int bufferLen = buffer.getPos();
                    buffer.reset();
                    String readKey = buffer.readString();
                    if (readKey.equals(goodId)) {  // 找到了记录
                        results.add(parserBuffer(buffer, bufferLen-readKey.length()-4, readKey));
                    }
                }
                if (pageId == -1) {   // 跳出循环
                    break;
                }
            }
        }
        return results;
    }

}
