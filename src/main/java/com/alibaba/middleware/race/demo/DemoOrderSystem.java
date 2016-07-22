package com.alibaba.middleware.race.demo;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.store.fs.RawFileHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class DemoOrderSystem implements OrderSystem {

  final List<String> comparableKeysOrderingByOrderId;

  final List<String> comparableKeysOrderingByBuyerCreateTimeOrderId;
  final List<String> comparableKeysOrderingBySalerGoodOrderId;
  final List<String> comparableKeysOrderingByGood;
  final List<String> comparableKeysOrderingByGoodOrderId;
  final List<String> comparableKeysOrderingByBuyer;

  static private class ComparableKeys implements Comparable<ComparableKeys> {
    List<String> orderingKeys;
    Record record;

    private ComparableKeys(List<String> orderingKeys, Record record) {
      if (orderingKeys == null || orderingKeys.size() == 0) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      this.orderingKeys = orderingKeys;
      this.record = record;
    }

    public int compareTo(ComparableKeys o) {
      if (this.orderingKeys.size() != o.orderingKeys.size()) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      for (String key : orderingKeys) {
        Field a = this.record.get(key);
        Field b = o.record.get(key);
        if (a == null || b == null) {
          throw new RuntimeException("Bad input data: " + key);
        }
        int ret = a.compareTo(b);
        if (ret != 0) {
          return ret;
        }
      }
      return 0;
    }
  }

  TreeMap<ComparableKeys, Record> orderDataSortedByOrder = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();
  TreeMap<ComparableKeys, Record> orderDataSortedByBuyerCreateTime = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();
  TreeMap<ComparableKeys, Record> orderDataSortedBySalerGood = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();
  TreeMap<ComparableKeys, Record> orderDataSortedByGood = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();
  TreeMap<ComparableKeys, Record> buyerDataStoredByBuyer = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();
  TreeMap<ComparableKeys, Record> goodDataStoredByGood = new TreeMap<DemoOrderSystem.ComparableKeys, Record>();

  public DemoOrderSystem() {
    comparableKeysOrderingByOrderId = new ArrayList<String>();
    comparableKeysOrderingByBuyerCreateTimeOrderId = new ArrayList<String>();
    comparableKeysOrderingBySalerGoodOrderId = new ArrayList<String>();
    comparableKeysOrderingByGood = new ArrayList<String>();
    comparableKeysOrderingByGoodOrderId = new ArrayList<String>();
    comparableKeysOrderingByBuyer = new ArrayList<String>();

    comparableKeysOrderingByOrderId.add("orderid");

    comparableKeysOrderingByBuyerCreateTimeOrderId.add("buyerid");
    comparableKeysOrderingByBuyerCreateTimeOrderId.add("createtime");
    comparableKeysOrderingByBuyerCreateTimeOrderId.add("orderid");

    comparableKeysOrderingBySalerGoodOrderId.add("salerid");
    comparableKeysOrderingBySalerGoodOrderId.add("goodid");
    comparableKeysOrderingBySalerGoodOrderId.add("orderid");

    comparableKeysOrderingByGoodOrderId.add("goodid");
    comparableKeysOrderingByGoodOrderId.add("orderid");

    comparableKeysOrderingByGood.add("goodid");

    comparableKeysOrderingByBuyer.add("buyerid");
  }

  public void construct(Collection<String> orderFiles,
      Collection<String> buyerFiles, Collection<String> goodFiles,
      Collection<String> storeFolders) throws IOException, InterruptedException {

    // Handling goodFiles
    new RawFileHandler() {
      @Override
      public void handleRow(Record record) {
        goodDataStoredByGood.put(new ComparableKeys(
            comparableKeysOrderingByGood, record), record);
      }
    }.handle(goodFiles);

    // Handling orderFiles
    new RawFileHandler() {
      @Override
      public void handleRow(Record record) {
        Field goodid = record.getKV("goodid");
        Record goodData = goodDataStoredByGood.get(new ComparableKeys(
                comparableKeysOrderingByGood, record));
        if (goodData == null) {
          throw new RuntimeException("Bad data! goodid " + goodid.rawValue
              + " not exist in good files");
        }
        Field salerid = goodData.get("salerid");
        record.put("salerid", salerid);

        orderDataSortedByOrder.put(new ComparableKeys(
            comparableKeysOrderingByOrderId, record), record);
        orderDataSortedByBuyerCreateTime.put(new ComparableKeys(
            comparableKeysOrderingByBuyerCreateTimeOrderId, record), record);
        orderDataSortedBySalerGood.put(new ComparableKeys(
            comparableKeysOrderingBySalerGoodOrderId, record), record);
        orderDataSortedByGood.put(new ComparableKeys(
            comparableKeysOrderingByGoodOrderId, record), record);
      }
    }.handle(orderFiles);

    // Handling buyerFiles
    new RawFileHandler() {
      @Override
      public void handleRow(Record record) {
        buyerDataStoredByBuyer.put(new ComparableKeys(
            comparableKeysOrderingByBuyer, record), record);
      }
    }.handle(buyerFiles);
  }

  public Result queryOrder(long orderId, Collection<String> keys) {
    Record query = new Record();
    query.putKV("orderid", orderId);

    Record orderData = orderDataSortedByOrder.get(new ComparableKeys(
        comparableKeysOrderingByOrderId, query));
    if (orderData == null) {
      return null;
    }

    return createResultFromOrderData(orderData, createQueryKeys(keys));
  }

  private ResultRecord createResultFromOrderData(Record orderData,
      Collection<String> keys) {
    Record buyerQuery = new Record(orderData.getKV("buyerid"));
    Record buyerData = buyerDataStoredByBuyer.get(new ComparableKeys(
        comparableKeysOrderingByBuyer, buyerQuery));

    Record goodQuery = new Record(orderData.getKV("goodid"));
    Record goodData = goodDataStoredByGood.get(new ComparableKeys(
        comparableKeysOrderingByGood, goodQuery));

    return ResultRecord.createResultRow(orderData, buyerData, goodData,
            createQueryKeys(keys));
  }

  private HashSet<String> createQueryKeys(Collection<String> keys) {
    if (keys == null) {
      return null;
    }
    return new HashSet<String>(keys);
  }

  public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
      String buyerid) {
    Record queryStart = new Record();
    queryStart.putKV("buyerid", buyerid);
    queryStart.putKV("createtime", startTime);
    queryStart.putKV("orderid", Long.MIN_VALUE);

    Record queryEnd = new Record();
    queryEnd.putKV("buyerid", buyerid);
    queryEnd.putKV("createtime", endTime - 1); // exclusive end
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Record> orders = orderDataSortedByBuyerCreateTime
        .subMap(new ComparableKeys(
            comparableKeysOrderingByBuyerCreateTimeOrderId, queryStart),
            new ComparableKeys(comparableKeysOrderingByBuyerCreateTimeOrderId,
                queryEnd));

    return new Iterator<OrderSystem.Result>() {

      SortedMap<ComparableKeys, Record> o = orders;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        ComparableKeys lastKey = o.lastKey();
        Record orderData = o.get(lastKey);
        o.remove(lastKey);
        return createResultFromOrderData(orderData, null);
      }

      public void remove() {

      }
    };
  }

  public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
      Collection<String> keys) {
    final Collection<String> queryKeys = keys;

    Record queryStart = new Record();
    queryStart.putKV("salerid", salerid);
    queryStart.putKV("goodid", goodid);
    queryStart.putKV("orderid", Long.MIN_VALUE);
    Record queryEnd = new Record();
    queryEnd.putKV("salerid", salerid);
    queryEnd.putKV("goodid", goodid);
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Record> orders = orderDataSortedBySalerGood
        .subMap(new ComparableKeys(comparableKeysOrderingBySalerGoodOrderId,
            queryStart), new ComparableKeys(
            comparableKeysOrderingBySalerGoodOrderId, queryEnd));

    return new Iterator<OrderSystem.Result>() {

      SortedMap<ComparableKeys, Record> o = orders;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        ComparableKeys firstKey = o.firstKey();
        Record orderData = o.get(firstKey);
        o.remove(firstKey);
        return createResultFromOrderData(orderData, createQueryKeys(queryKeys));
      }

      public void remove() {
        // ignore
      }
    };
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {
    Record queryStart = new Record();
    queryStart.putKV("goodid", goodid);
    queryStart.putKV("orderid", Long.MIN_VALUE);
    Record queryEnd = new Record();
    queryEnd.putKV("goodid", goodid);
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Record> ordersData = orderDataSortedByGood
        .subMap(new ComparableKeys(comparableKeysOrderingByGoodOrderId,
            queryStart), new ComparableKeys(
            comparableKeysOrderingByGoodOrderId, queryEnd));
    if (ordersData == null || ordersData.isEmpty()) {
      return null;
    }

    HashSet<String> queryingKeys = new HashSet<String>();
    queryingKeys.add(key);
    List<ResultRecord> allData = new ArrayList<ResultRecord>(ordersData.size());
    for (Record orderData : ordersData.values()) {
      allData.add(createResultFromOrderData(orderData, queryingKeys));
    }

    // accumulate as Long
    try {
      boolean hasValidData = false;
      long sum = 0;
      for (ResultRecord r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsLong();
          hasValidData = true;
        }
      }
      if (hasValidData) {
        return new Field(key, Long.toString(sum));
      }
    } catch (TypeException e) {
    }

    // accumulate as double
    try {
      boolean hasValidData = false;
      double sum = 0;
      for (ResultRecord r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsDouble();
          hasValidData = true;
        }
      }
      if (hasValidData) {
        return new Field(key, Double.toString(sum));
      }
    } catch (TypeException e) {
    }

    return null;
  }
}
