package com.alibaba.middleware.race;

import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.result.KVImpl;
import com.alibaba.middleware.race.result.ResultImpl;
import com.alibaba.middleware.race.result.ResultIterator;
import com.alibaba.middleware.race.table.*;
import com.alibaba.middleware.race.worker.WorkerManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 订单系统的实现
 */
public class OrderSystemImpl implements OrderSystem {

    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        WorkerManager manager = new WorkerManager();
        manager.setStoreFolders(storeFolders);
        manager.setBuyerFiles(buyerFiles);
        manager.setGoodFiles(goodFiles);
        manager.setOrderFiles(orderFiles);

        // 打印目录信息
        printDir(orderFiles, "orderFiles");
        printDir(buyerFiles, "buyerFiles");
        printDir(goodFiles, "goodFiles");
        printDir(storeFolders, "storeFolders");

        Thread managerThread = new Thread(manager);
        managerThread.setDaemon(true);
        managerThread.start();

        // 等待构造完成
        while (!OrderTable.getInstance().isPrepared()) {
            if ((System.currentTimeMillis()-beginTime) > (60*60000-3000)) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void printDir(Collection<String> dir, String name) {
        System.out.println(name + "======>>>>>");
        for (String file: dir) {
            System.out.println(file);
        }
    }


    private void waitForPrepared() {
        while (!OrderTable.getInstance().isPrepared()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 查询订单号为orderid的指定字段
     * 实现：根据keys判断需要查询那几张表，再根据OrderId进行Hash查找索引，再根据索引读入对应的块，解析出对应的数据，
     *      再根据解析出来的数据使用hash查找Good表和Buyer表
     *
     * @param orderId
     *          订单号
     * @param keys
     *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
     * @return 查询结果，如果该订单不存在，返回null
     */
    public Result queryOrder(long orderId, Collection<String> keys) {
        waitForPrepared();

        // 查找订单表
        RecordIndex orderIdRowIndex = OrderTable.getInstance().findOderIdIndex(orderId);
        if (orderIdRowIndex == null) {
            return null;
        }
        if (keys != null && keys.size()==0) {
            return new ResultImpl(orderId, new HashMap<String, KVImpl>());
        }
        // 查询字段分类
        ArrayList<String> orderTableKeys = null;
        ArrayList<String> goodTableKeys = null;
        ArrayList<String> buyerTableKeys = null;
        if (keys != null) {
            Table goodTable = GoodTable.getInstance().baseTable;
            Table buyerTable = BuyerTable.getInstance().baseTable;
            orderTableKeys = new ArrayList<String>();
            goodTableKeys = new ArrayList<String>();
            buyerTableKeys = new ArrayList<String>();
            for (String key: keys) {
                if (goodTable.containColumn(key)) {
                    goodTableKeys.add(key);
                } else if (buyerTable.containColumn(key)) {
                    buyerTableKeys.add(key);
                } else {
                    orderTableKeys.add(key);
                }
            }
        }
        if (goodTableKeys != null && goodTableKeys.size() == 1 && goodTableKeys.contains("goodid")) {
            orderTableKeys.add("goodid");
            goodTableKeys.clear();
        }
        if (buyerTableKeys != null && buyerTableKeys.size() == 1 && buyerTableKeys.contains("buyerid")) {
            orderTableKeys.add("buyerid");
            buyerTableKeys.clear();
        }

        HashMap<String, String> goodRecord = null;
        HashMap<String, String> buyerRecord = null;
        HashMap<String, String> orderRecord = OrderTable.getInstance().findOrderRecord(orderIdRowIndex);
        CountDownLatch waitForGood = null;
        CountDownLatch waitForBuyer = null;
        if (goodTableKeys == null || goodTableKeys.size() > 0) {
            String goodId = orderRecord.get("goodid");
            goodRecord = new HashMap<String, String>();
            waitForGood = new CountDownLatch(1);
            GoodTable.instance.findGood(goodId, waitForGood, goodRecord);
        }
        if (buyerTableKeys == null || buyerTableKeys.size() > 0) {
            String buyerId = orderRecord.get("buyerid");
            buyerRecord = new HashMap<String, String>();
            waitForBuyer = new CountDownLatch(1);
            BuyerTable.instance.findBuyer(buyerId, waitForBuyer, buyerRecord);
        }
        if (waitForBuyer != null) {
            try {
                waitForBuyer.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (waitForGood != null) {
            try {
                waitForGood.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        HashMap<String, KVImpl> result;
        if (keys == null) {
            result = joinResult(orderRecord, buyerRecord, goodRecord);
        } else {
            result = new HashMap<String, KVImpl>(keys.size());
            String value;
            for (String key: orderTableKeys) {
                value = orderRecord.get(key);
                if (value != null) {
                    KVImpl kv = new KVImpl(key, value);
                    result.put(key, kv);
                }
            }
            for (String key: goodTableKeys) {
                value = goodRecord.get(key);
                if (value != null) {
                    KVImpl kv = new KVImpl(key, value);
                    result.put(key, kv);
                }
            }
            for (String key: buyerTableKeys) {
                value = buyerRecord.get(key);
                if (value != null) {
                    KVImpl kv = new KVImpl(key, value);
                    result.put(key, kv);
                }
            }
        }
        return new ResultImpl(orderId, result);
    }

    /**
     * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
     *
     * @param startTime 订单创建时间的下界
     * @param endTime 订单创建时间的上界
     * @param buyerId
     *          买家Id
     * @return 符合条件的订单集合，按照createtime大到小排列
     */
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerId) {
        waitForPrepared();

        return new ResultIterator(OrderTable.getInstance().findByBuyer(buyerId, startTime, endTime));

    }

    /**
     * 查询某位卖家某件商品所有订单的某些字段
     *
     * @param salerId 卖家Id
     * @param goodId 商品Id
     * @param keys 待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
     * @return 符合条件的订单集合，按照订单id从小至大排序
     */
    public Iterator<Result> queryOrdersBySaler(String salerId, String goodId,
      Collection<String> keys) {
        waitForPrepared();

        ArrayList<RecordIndex> goodRowIndex = OrderTable.getInstance().findGoodIdIndex(goodId);
        if (goodRowIndex.isEmpty()) {  // 没有销售记录
            return new ResultIterator(new ArrayList<ResultImpl>());
        }

        // 查询字段分类
        ArrayList<String> orderTableKeys = null;
        ArrayList<String> goodTableKeys = null;
        ArrayList<String> buyerTableKeys = null;
        if (keys != null) {
            Table goodTable = GoodTable.getInstance().baseTable;
            Table buyerTable = BuyerTable.getInstance().baseTable;
            orderTableKeys = new ArrayList<String>();
            goodTableKeys = new ArrayList<String>();
            buyerTableKeys = new ArrayList<String>();
            for (String key: keys) {
                if (goodTable.containColumn(key)) {
                    goodTableKeys.add(key);
                } else if (buyerTable.containColumn(key)) {
                    buyerTableKeys.add(key);
                } else {
                    orderTableKeys.add(key);
                }
            }
        }

        if (goodTableKeys != null && goodTableKeys.size() == 1 && goodTableKeys.contains("goodid")) {
            orderTableKeys.add("goodid");
            goodTableKeys.clear();
        }
        if (buyerTableKeys != null && buyerTableKeys.size() == 1 && buyerTableKeys.contains("buyerid")) {
            orderTableKeys.add("buyerid");
            buyerTableKeys.clear();
        }

        ArrayList<HashMap<String, String>>  orderRecords = OrderTable.getInstance().findOrders(goodRowIndex);

        HashMap<String, String> goodRecord = null;
        CountDownLatch waitForGood = null;
        if (goodTableKeys == null || goodTableKeys.size() > 0) {
            goodRecord = new HashMap<String, String>();
            waitForGood = new CountDownLatch(1);
            GoodTable.getInstance().findGood(goodId, waitForGood, goodRecord);
        }

        HashMap<String, HashMap<String, String>> buyerRecords = null;
        if (buyerTableKeys == null || buyerTableKeys.size() > 0) {
            buyerRecords = BuyerTable.getInstance().findBuyerOfOrder(orderRecords);
        }

        if (waitForGood != null) {
            try {
                waitForGood.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ArrayList<ResultImpl> results = new ArrayList<ResultImpl>(orderRecords.size());
        HashMap<String, KVImpl> result;
        HashMap<String, String> buyerRecord = null;
        for (HashMap<String, String> orderRecord : orderRecords) {
            if (buyerRecords != null) {
                buyerRecord = buyerRecords.get(orderRecord.get("buyerid"));
            }
            if (keys == null) {
                result = joinResult(orderRecord, buyerRecord, goodRecord);
            } else {
                result = new HashMap<String, KVImpl>();
                String value;
                for (String key: orderTableKeys) {
                    value = orderRecord.get(key);
                    if (value != null) {
                        KVImpl kv = new KVImpl(key, value);
                        result.put(key, kv);
                    }
                }
                for (String key: goodTableKeys) {
                    value = goodRecord.get(key);
                    if (value != null) {
                        KVImpl kv = new KVImpl(key, value);
                        result.put(key, kv);
                    }
                }
                for (String key: buyerTableKeys) {
                    value = buyerRecord.get(key);
                    if (value != null) {
                        KVImpl kv = new KVImpl(key, value);
                        result.put(key, kv);
                    }
                }
            }
            results.add(new ResultImpl(Long.parseLong(orderRecord.get("orderid")), result));
        }
        return new ResultIterator(results);
    }

    /**
     * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
     * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
     *
     * @param goodId 商品Id
     * @param key 求和字段
     * @return 求和结果
     */
    public KeyValue sumOrdersByGood(String goodId, String key) {
        waitForPrepared();

        ArrayList<RecordIndex> goodRowIndex = OrderTable.getInstance().findGoodIdIndex(goodId);
        if (goodRowIndex.isEmpty()) {
            return null;
        }
        HashMap<String, String> goodRecord;
        if (GoodTable.getInstance().baseTable.containColumn(key)) {
            goodRecord = GoodTable.getInstance().findFromFile(goodId);
            String value = goodRecord.get(key);
            if (value == null) {
                return null;
            } else  {
                try {
                    return new KVImpl (key, Long.toString(goodRowIndex.size()*Long.parseLong(value)));
                } catch (NumberFormatException e) {
                    try {
                        return new KVImpl (key, Double.toString(goodRowIndex.size()*Double.parseDouble(value)));
                    } catch (NumberFormatException e2) {
                        return null;
                    }
                }
            }
        }
        long sumLong = 0;
        double sumDouble = 0.0;
        boolean hasLong = false;
        boolean hasDouble = false;
        boolean keyInBuyerTable = BuyerTable.getInstance().baseTable.containColumn(key);
        ArrayList<HashMap<String, String>>  orderRecords = OrderTable.getInstance().findOrders(goodRowIndex);
        HashMap<String, HashMap<String, String>> buyerRecords = null;
        if (keyInBuyerTable) {
            buyerRecords = BuyerTable.getInstance().findBuyerOfOrder(orderRecords);
        }
        for (HashMap<String, String> order : orderRecords) {
            Object value;
            if (!keyInBuyerTable) {
                value = order.get(key);
            } else {
                HashMap<String, String> buyerRecord = buyerRecords.get(order.get("buyerid"));
                value = buyerRecord.get(key);
            }
            if (value != null) {
                try {
                    hasLong = true;
                    sumLong +=  Long.parseLong(((String) value));
                } catch (NumberFormatException e) {
                    try {
                        hasDouble = true;
                        sumDouble += Double.parseDouble(((String) value));
                    } catch (NumberFormatException e2) {
                        return null;
                    }
                }
            }
        }
        if (hasDouble) {
            sumDouble += sumLong;
            return new KVImpl(key, Double.toString(sumDouble));
        } else if (hasLong) {
            return new KVImpl(key, Long.toString(sumLong));
        } else {
            return null;
        }
    }

    public static HashMap<String, KVImpl> joinResult(HashMap<String, String> orderRecord,
                                                 HashMap<String, String> buyerRecord,
                                                 HashMap<String, String> goodRecord) {
        HashMap<String, KVImpl> result = new HashMap<String, KVImpl>(
                (int)Math.ceil((orderRecord.size()+buyerRecord.size()+goodRecord.size())/0.75));
        for (Map.Entry<String, String> entry: orderRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry: buyerRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, String> entry: goodRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        return result;
    }

}
