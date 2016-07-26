package com.alibaba.middleware.race;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.result.KVImpl;
import com.alibaba.middleware.race.result.ResultImpl;
import com.alibaba.middleware.race.result.ResultIterator;
import com.alibaba.middleware.race.table.*;
import com.alibaba.middleware.race.worker.WorkerManager;

import java.io.IOException;
import java.util.*;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

//    private HashMap<String, Column> orderColumns = new HashMap<String, Column>();
//    private HashMap<String, Column> goodColumns = new HashMap<String, Column>();
//    private HashMap<String, Column> buyerColumns = new HashMap<String, Column>();

    /*
    1. 建立存储处理后数据需要的相关文件
       文件份为以下三种：
       a. 按照GoodId的Hash值存储的Good.db
       b. 按照BuyerId的Hash值存储的Buyer.db
       c. 按照BuyerId, OrderTime顺序存储的Order.db
    2. 先处理商品文件，再处理买家文件，最后处理订单文件

    3. 商品文件/买家文件
       读入原始文件 --> 对goodId/buyerId进行hash  -> 存入相应的块中

    4. 订单文件
       读入原始文件

     */
    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {
        WorkerManager manager = new WorkerManager();
        manager.setStoreFolders(storeFolders);
        manager.setBuyerFiles(buyerFiles);
        manager.setGoodFiles(goodFiles);
        manager.setOrderFiles(orderFiles);

        printDir(orderFiles, "orderFiles");
        printDir(buyerFiles, "buyerFiles");
        printDir(goodFiles, "goodFiles");
        printDir(storeFolders, "storeFolders");

        manager.run();
    }

    private void printDir(Collection<String> dir, String name) {
        System.out.println(name + "======>>>>>");
        for (String file: dir) {
            System.out.println(file);
        }
    }


    /**
     * 查询订单号为orderid的指定字段
     * 实现：根据keys判断需要查询那几张表，再根据OrderId进行Hash查找索引，再根据索引读入对应的块，解析出对应的数据，
     *      再根据解析出来的数据使用hash查找Good表和Buyer表
     *      TODO 索引按照[orderId, goodId, buyerId] 存储，当不需要Order表字段时没必要读Order表
     *
     * @param orderId
     *          订单号
     * @param keys
     *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
     * @return 查询结果，如果该订单不存在，返回null
     */
    public Result queryOrder(long orderId, Collection<String> keys) {
//        TreeMap<Integer, String> orderKeys = null;
//        TreeMap<Integer, String> goodKeys = null;
//        TreeMap<Integer, String> buyerKeys = null;
//        if (keys != null) {
//            orderKeys = new TreeMap<Integer, String>();
//            goodKeys = new TreeMap<Integer, String>();
//            buyerKeys = new TreeMap<Integer, String>();
//            keyOfTable(keys, orderKeys, goodKeys, buyerKeys);
//        }
        // 查找订单表
        OrderIdRowIndex orderIdRowIndex = OrderTable.getInstance().findOderIdIndex(orderId);
        if (orderIdRowIndex == null) {
            return null;
        }
        HashMap<String, Object> orderRecord = OrderTable.getInstance().findOrders(orderIdRowIndex);
//        if (orderRecord.isEmpty()) {
//            throw new RuntimeException("找到了索引找不到记录");
//        }
//        for (Map.Entry<String, Object> kv : orderRecord.entrySet()) {
//            System.out.println(kv.toString().replace("=", ":"));
//        }
        String buyerId = ((String) orderRecord.get("buyerid"));
        HashMap<String, Object> buyerRecord = BuyerTable.getInstance().find(buyerId);
//        if (buyerRecord.isEmpty()) {
//            throw new RuntimeException("找不到买家记录");
//        }
//        for (Map.Entry<String, Object> kv : buyerRecord.entrySet()) {
//            System.out.println(kv.toString().replace("=", ":"));
//        }
        String goodId = ((String) orderRecord.get("goodid"));
        HashMap<String, Object> goodRecord = GoodTable.getInstance().find(goodId);
//        if (goodRecord.isEmpty()) {
//            throw new RuntimeException("找不到商品记录");
//        }
//        for (Map.Entry<String, Object> kv : goodRecord.entrySet()) {
//            System.out.println(kv.toString().replace("=", ":"));
//        }
        HashMap<String, KVImpl> result;
        if (keys == null) {
            result = joinResult(orderRecord, buyerRecord, goodRecord);
        } else {
            result = new HashMap<String, KVImpl>();
            for (String key: keys) {
                Object value = orderRecord.get(key);
                if (value == null) {
                    value = goodRecord.get(key);
                    if (value == null) {
                        value = buyerRecord.get(key);
                    }
                }
                if (value != null) {
                    KVImpl kv = new KVImpl(key, value);
                    result.put(key, kv);
                }
            }
        }
        return new ResultImpl(((Long) orderRecord.get("orderid")), result);
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
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
      String buyerId) {
        ArrayList<ResultImpl> results = new ArrayList<ResultImpl>();
        ArrayList<BuyerIdRowIndex> buyerIdRowIndices = OrderTable.getInstance()
                .findBuyerIdIndex(buyerId, startTime, endTime);
        HashMap<String, Object> buyerRecord = BuyerTable.getInstance().find(buyerId);
        ArrayList<HashMap<String, Object>>  orderRecords = OrderTable.getInstance().findOrders(buyerIdRowIndices);
        for (HashMap<String, Object> order : orderRecords) {
            String goodId = ((String) order.get("goodid"));
            HashMap<String, Object> goodRecord = GoodTable.getInstance().find(goodId);
            HashMap<String, KVImpl> result = joinResult(order, buyerRecord, goodRecord);
            results.add(new ResultImpl(((Long) order.get("orderid")), result, ((Long) order.get("createtime"))));
        }
        return new ResultIterator(results);
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
        ArrayList<ResultImpl> results = new ArrayList<ResultImpl>();
        HashMap<String, Object> goodRecord = GoodTable.getInstance().find(goodId);
        ArrayList<HashMap<String, Object>>  orderRecords = OrderTable.getInstance().findOrders(goodId);
        for (HashMap<String, Object> order : orderRecords) {
            String buyerId = ((String) order.get("buyerid"));
            HashMap<String, Object> buyerRecord = BuyerTable.getInstance().find(buyerId);
            HashMap<String, KVImpl> result;
            if (keys == null) {
                result = joinResult(order, buyerRecord, goodRecord);
            } else {
                result = new HashMap<String, KVImpl>();
                for (String key: keys) {
                    Object value = order.get(key);
                    if (value == null) {
                        value = goodRecord.get(key);
                        if (value == null) {
                            value = buyerRecord.get(key);
                        }
                    }
                    if (value != null) {
                        KVImpl kv = new KVImpl(key, value);
                        result.put(key, kv);
                    }
                }
            }
            results.add(new ResultImpl(((Long) order.get("orderid")), result));
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
        long sumLong = 0;
        double sumDouble = 0.0;
        boolean hasLong = false;
        boolean hasDouble = false;
        ArrayList<HashMap<String, Object>>  orderRecords = OrderTable.getInstance().findOrders(goodId);
        HashMap<String, Object> goodRecord = GoodTable.getInstance().find(goodId);
        for (HashMap<String, Object> order : orderRecords) {
            Object value = order.get(key);
            if (value == null) {
                value = goodRecord.get(key);
                if (value == null) {
                    String buyerId = ((String) order.get("buyerid"));
                    HashMap<String, Object> buyerRecord = BuyerTable.getInstance().find(buyerId);
                    value = buyerRecord.get(key);
                }
            }
            if (value != null) {
                if (value instanceof Long) {
                    hasLong = true;
                    sumLong += (Long)value;
                } else if(value instanceof Double) {
                    hasDouble = true;
                    sumDouble += ((Double) value);
                } else if (value instanceof String) {
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
                else {
                    return null;
                }
            }
        }
        if (hasDouble) {
            sumDouble += sumLong;
            return new KVImpl(key, sumDouble);
        } else if (hasLong) {
            return new KVImpl(key, sumLong);
        } else {
            return null;
        }
    }

    private HashMap<String, KVImpl> joinResult(HashMap<String, Object> orderRecord,
                                                 HashMap<String, Object> buyerRecord,
                                                 HashMap<String, Object> goodRecord) {
        HashMap<String, KVImpl> result = new HashMap<String, KVImpl>();
        for (Map.Entry<String, Object> entry: orderRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Object> entry: buyerRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        for (Map.Entry<String, Object> entry: goodRecord.entrySet()) {
            result.put(entry.getKey(), new KVImpl(entry.getKey(), entry.getValue()));
        }
        return result;
    }


    // 找到各表的字段
//    private void keyOfTable(Collection<String> keys, TreeMap<Integer, String> orderKeys,
//                            TreeMap<Integer, String> goodKeys, TreeMap<Integer, String> buyerKeys) {
//        Column column;
//        for(String key: keys) {
//            column = orderColumns.get(key);
//            if (column != null) {
//                orderKeys.put(column.getColumnId(), key);
//            } else {
//                column = goodColumns.get(key);
//                if (column != null) {
//                    goodKeys.put(column.getColumnId(), key);
//                } else {
//                    column = buyerColumns.get(key);
//                    if (column != null) {
//                        buyerKeys.put(column.getColumnId(), key);
//                    }
//                }
//            }
//        }
//    }
}
