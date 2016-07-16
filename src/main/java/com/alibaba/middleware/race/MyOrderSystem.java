package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.*;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class MyOrderSystem implements OrderSystem {

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

    public Result queryOrder(long orderId, Collection<String> keys) {
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
}
