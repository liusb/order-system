package com.alibaba.middleware.race;

import com.alibaba.middleware.race.table.Column;

import java.io.IOException;
import java.util.*;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class MyOrderSystem implements OrderSystem {

    private HashMap<String, Column> orderColumns = new HashMap<String, Column>();
    private HashMap<String, Column> goodColumns = new HashMap<String, Column>();
    private HashMap<String, Column> buyerColumns = new HashMap<String, Column>();

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
        TreeMap<Integer, String> orderKeys = null;
        TreeMap<Integer, String> goodKeys = null;
        TreeMap<Integer, String> buyerKeys = null;
        if (keys != null) {
            orderKeys = new TreeMap<Integer, String>();
            goodKeys = new TreeMap<Integer, String>();
            buyerKeys = new TreeMap<Integer, String>();
            keyOfTable(keys, orderKeys, goodKeys, buyerKeys);
        }
        // 查找订单表


      return null;
    }



    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
      String buyerId) {
        return null;
    }

    public Iterator<Result> queryOrdersBySaler(String salerId, String goodId,
      Collection<String> keys) {
        return null;
    }

    public KeyValue sumOrdersByGood(String goodid, String key) {
        return null;
    }


    // 找到各表的字段
    private void keyOfTable(Collection<String> keys, TreeMap<Integer, String> orderKeys,
                            TreeMap<Integer, String> goodKeys, TreeMap<Integer, String> buyerKeys) {
        Column column;
        for(String key: keys) {
            column = orderColumns.get(key);
            if (column != null) {
                orderKeys.put(column.getColumnId(), key);
            } else {
                column = goodColumns.get(key);
                if (column != null) {
                    goodKeys.put(column.getColumnId(), key);
                } else {
                    column = buyerColumns.get(key);
                    if (column != null) {
                        buyerKeys.put(column.getColumnId(), key);
                    }
                }
            }
        }
    }
}
