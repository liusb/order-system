package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

  static private String booleanTrueValue = "true";
  static private String booleanFalseValue = "false";

  final List<String> comparableKeysOrderingByOrderId;

  final List<String> comparableKeysOrderingByBuyerCreateTimeOrderId;
  final List<String> comparableKeysOrderingBySalerGoodOrderId;
  final List<String> comparableKeysOrderingByGood;
  final List<String> comparableKeysOrderingByGoodOrderId;
  final List<String> comparableKeysOrderingByBuyer;

  static private class KV implements Comparable<KV>, KeyValue {
    String key;
    String rawValue;

    boolean isComparableLong = false;
    long longValue;

    private KV(String key, String rawValue) {
      this.key = key;
      this.rawValue = rawValue;
      if (key.equals("createtime") || key.equals("orderid")) {
        isComparableLong = true;
        longValue = Long.parseLong(rawValue);
      }
    }

    public String key() {
      return key;
    }

    public String valueAsString() {
      return rawValue;
    }

    public long valueAsLong() throws TypeException {
      try {
        return Long.parseLong(rawValue);
      } catch (NumberFormatException e) {
        throw new TypeException();
      }
    }

    public double valueAsDouble() throws TypeException {
      try {
        return Double.parseDouble(rawValue);
      } catch (NumberFormatException e) {
        throw new TypeException();
      }
    }

    public boolean valueAsBoolean() throws TypeException {
      if (this.rawValue.equals(booleanTrueValue)) {
        return true;
      }
      if (this.rawValue.equals(booleanFalseValue)) {
        return false;
      }
      throw new TypeException();
    }

    public int compareTo(KV o) {
      if (!this.key().equals(o.key())) {
        throw new RuntimeException("Cannot compare from different key");
      }
      if (isComparableLong) {
        return Long.compare(this.longValue, o.longValue);
      }
      return this.rawValue.compareTo(o.rawValue);
    }

    @Override
    public String toString() {
      return "[" + this.key + "]:" + this.rawValue;
    }
  }

  @SuppressWarnings("serial")
  static private class Row extends HashMap<String, KV> {
    Row() {
      super();
    }

    Row(KV kv) {
      super();
      this.put(kv.key(), kv);
    }

    KV getKV(String key) {
      KV kv = this.get(key);
      if (kv == null) {
        throw new RuntimeException(key + " is not exist");
      }
      return kv;
    }

    Row putKV(String key, String value) {
      KV kv = new KV(key, value);
      this.put(kv.key(), kv);
      return this;
    }

    Row putKV(String key, long value) {
      KV kv = new KV(key, Long.toString(value));
      this.put(kv.key(), kv);
      return this;
    }
  }

  private static class ResultImpl implements Result {
    private long orderid;
    private Row kvMap;

    private ResultImpl(long orderid, Row kv) {
      this.orderid = orderid;
      this.kvMap = kv;
    }

    static private ResultImpl createResultRow(Row orderData, Row buyerData,
        Row goodData, Set<String> queryingKeys) {
      if (orderData == null || buyerData == null || goodData == null) {
        throw new RuntimeException("Bad data!");
      }
      Row allkv = new Row();
      long orderid;
      try {
        orderid = orderData.get("orderid").valueAsLong();
      } catch (TypeException e) {
        throw new RuntimeException("Bad data!");
      }

      for (KV kv : orderData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : buyerData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : goodData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      return new ResultImpl(orderid, allkv);
    }

    public KeyValue get(String key) {
      return this.kvMap.get(key);
    }

    public KeyValue[] getAll() {
      return kvMap.values().toArray(new KeyValue[0]);
    }

    public long orderId() {
      return orderid;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("orderid: " + orderid + " {");
      if (kvMap != null && !kvMap.isEmpty()) {
        for (KV kv : kvMap.values()) {
          sb.append(kv.toString());
          sb.append(",\n");
        }
      }
      sb.append('}');
      return sb.toString();
    }
  }

  static private class ComparableKeys implements Comparable<ComparableKeys> {
    List<String> orderingKeys;
    Row row;

    private ComparableKeys(List<String> orderingKeys, Row row) {
      if (orderingKeys == null || orderingKeys.size() == 0) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      this.orderingKeys = orderingKeys;
      this.row = row;
    }

    public int compareTo(ComparableKeys o) {
      if (this.orderingKeys.size() != o.orderingKeys.size()) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      for (String key : orderingKeys) {
        KV a = this.row.get(key);
        KV b = o.row.get(key);
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

  TreeMap<ComparableKeys, Row> orderDataSortedByOrder = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> orderDataSortedByBuyerCreateTime = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> orderDataSortedBySalerGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> orderDataSortedByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> buyerDataStoredByBuyer = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> goodDataStoredByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();

  public OrderSystemImpl() {
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

  public static void main(String[] args) throws IOException,
      InterruptedException {

    // init order system
    List<String> orderFiles = new ArrayList<String>();
    List<String> buyerFiles = new ArrayList<String>();
    ;
    List<String> goodFiles = new ArrayList<String>();
    List<String> storeFolders = new ArrayList<String>();

    orderFiles.add("order_records.txt");
    buyerFiles.add("buyer_records.txt");
    goodFiles.add("good_records.txt");
    storeFolders.add("./");

    OrderSystem os = new OrderSystemImpl();
    os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

    // 用例
    long orderid = 2982388;
    System.out.println("\n查询订单号为" + orderid + "的订单");
    System.out.println(os.queryOrder(orderid, null));

    System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
    System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

    System.out.println("\n查询订单号为" + orderid
        + "的订单的contactphone, buyerid, foo, done, price字段");
    List<String> queryingKeys = new ArrayList<String>();
    queryingKeys.add("contactphone");
    queryingKeys.add("buyerid");
    queryingKeys.add("foo");
    queryingKeys.add("done");
    queryingKeys.add("price");
    Result result = os.queryOrder(orderid, queryingKeys);
    System.out.println(result);
    System.out.println("\n查询订单号不存在的订单");
    result = os.queryOrder(1111, queryingKeys);
    if (result == null) {
      System.out.println(1111 + " order not exist");
    }

    String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
    long startTime = 1471025622;
    long endTime = 1471219509;
    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
    Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
    while (it.hasNext()) {
      System.out.println(it.next());
    }

    String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
    String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
    System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
    it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
    while (it.hasNext()) {
      System.out.println(it.next());
    }

    goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
    String attr = "app_order_33_0";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    System.out.println(os.sumOrdersByGood(goodid, attr));

    attr = "done";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    KeyValue sum = os.sumOrdersByGood(goodid, attr);
    if (sum == null) {
      System.out.println("由于该字段是布尔类型，返回值是null");
    }

    attr = "foo";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    sum = os.sumOrdersByGood(goodid, attr);
    if (sum == null) {
      System.out.println("由于该字段不存在，返回值是null");
    }
  }

  private BufferedReader createReader(String file) throws FileNotFoundException {
    return new BufferedReader(new FileReader(file));
  }

  private Row createKVMapFromLine(String line) {
    String[] kvs = line.split("\t");
    Row kvMap = new Row();
    for (String rawkv : kvs) {
      int p = rawkv.indexOf(':');
      String key = rawkv.substring(0, p);
      String value = rawkv.substring(p + 1);
      if (key.length() == 0 || value.length() == 0) {
        throw new RuntimeException("Bad data:" + line);
      }
      KV kv = new KV(key, value);
      kvMap.put(kv.key(), kv);
    }
    return kvMap;
  }

  private abstract class DataFileHandler {
    abstract void handleRow(Row row);

    void handle(Collection<String> files) throws IOException {
      for (String file : files) {
        BufferedReader bfr = createReader(file);
        try {
          String line = bfr.readLine();
          while (line != null) {
            Row kvMap = createKVMapFromLine(line);
            handleRow(kvMap);
            line = bfr.readLine();
          }
        } finally {
          bfr.close();
        }
      }
    }
  }

  public void construct(Collection<String> orderFiles,
      Collection<String> buyerFiles, Collection<String> goodFiles,
      Collection<String> storeFolders) throws IOException, InterruptedException {

    // Handling goodFiles
    new DataFileHandler() {
      @Override
      void handleRow(Row row) {
        goodDataStoredByGood.put(new ComparableKeys(
            comparableKeysOrderingByGood, row), row);
      }
    }.handle(goodFiles);

    // Handling orderFiles
    new DataFileHandler() {
      @Override
      void handleRow(Row row) {
        KV goodid = row.getKV("goodid");
        Row goodData = goodDataStoredByGood.get(new ComparableKeys(
            comparableKeysOrderingByGood, row));
        if (goodData == null) {
          throw new RuntimeException("Bad data! goodid " + goodid.rawValue
              + " not exist in good files");
        }
        KV salerid = goodData.get("salerid");
        row.put("salerid", salerid);

        orderDataSortedByOrder.put(new ComparableKeys(
            comparableKeysOrderingByOrderId, row), row);
        orderDataSortedByBuyerCreateTime.put(new ComparableKeys(
            comparableKeysOrderingByBuyerCreateTimeOrderId, row), row);
        orderDataSortedBySalerGood.put(new ComparableKeys(
            comparableKeysOrderingBySalerGoodOrderId, row), row);
        orderDataSortedByGood.put(new ComparableKeys(
            comparableKeysOrderingByGoodOrderId, row), row);
      }
    }.handle(orderFiles);

    // Handling buyerFiles
    new DataFileHandler() {
      @Override
      void handleRow(Row row) {
        buyerDataStoredByBuyer.put(new ComparableKeys(
            comparableKeysOrderingByBuyer, row), row);
      }
    }.handle(buyerFiles);
  }

  public Result queryOrder(long orderId, Collection<String> keys) {
    Row query = new Row();
    query.putKV("orderid", orderId);

    Row orderData = orderDataSortedByOrder.get(new ComparableKeys(
        comparableKeysOrderingByOrderId, query));
    if (orderData == null) {
      return null;
    }

    return createResultFromOrderData(orderData, createQueryKeys(keys));
  }

  private ResultImpl createResultFromOrderData(Row orderData,
      Collection<String> keys) {
    Row buyerQuery = new Row(orderData.getKV("buyerid"));
    Row buyerData = buyerDataStoredByBuyer.get(new ComparableKeys(
        comparableKeysOrderingByBuyer, buyerQuery));

    Row goodQuery = new Row(orderData.getKV("goodid"));
    Row goodData = goodDataStoredByGood.get(new ComparableKeys(
        comparableKeysOrderingByGood, goodQuery));

    return ResultImpl.createResultRow(orderData, buyerData, goodData,
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
    Row queryStart = new Row();
    queryStart.putKV("buyerid", buyerid);
    queryStart.putKV("createtime", startTime);
    queryStart.putKV("orderid", Long.MIN_VALUE);

    Row queryEnd = new Row();
    queryEnd.putKV("buyerid", buyerid);
    queryEnd.putKV("createtime", endTime - 1); // exclusive end
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Row> orders = orderDataSortedByBuyerCreateTime
        .subMap(new ComparableKeys(
            comparableKeysOrderingByBuyerCreateTimeOrderId, queryStart),
            new ComparableKeys(comparableKeysOrderingByBuyerCreateTimeOrderId,
                queryEnd));

    return new Iterator<OrderSystem.Result>() {

      SortedMap<ComparableKeys, Row> o = orders;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        ComparableKeys lastKey = o.lastKey();
        Row orderData = o.get(lastKey);
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

    Row queryStart = new Row();
    queryStart.putKV("salerid", salerid);
    queryStart.putKV("goodid", goodid);
    queryStart.putKV("orderid", Long.MIN_VALUE);
    Row queryEnd = new Row();
    queryEnd.putKV("salerid", salerid);
    queryEnd.putKV("goodid", goodid);
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Row> orders = orderDataSortedBySalerGood
        .subMap(new ComparableKeys(comparableKeysOrderingBySalerGoodOrderId,
            queryStart), new ComparableKeys(
            comparableKeysOrderingBySalerGoodOrderId, queryEnd));

    return new Iterator<OrderSystem.Result>() {

      SortedMap<ComparableKeys, Row> o = orders;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        ComparableKeys firstKey = o.firstKey();
        Row orderData = o.get(firstKey);
        o.remove(firstKey);
        return createResultFromOrderData(orderData, createQueryKeys(queryKeys));
      }

      public void remove() {
        // ignore
      }
    };
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {
    Row queryStart = new Row();
    queryStart.putKV("goodid", goodid);
    queryStart.putKV("orderid", Long.MIN_VALUE);
    Row queryEnd = new Row();
    queryEnd.putKV("goodid", goodid);
    queryEnd.putKV("orderid", Long.MAX_VALUE);

    final SortedMap<ComparableKeys, Row> ordersData = orderDataSortedByGood
        .subMap(new ComparableKeys(comparableKeysOrderingByGoodOrderId,
            queryStart), new ComparableKeys(
            comparableKeysOrderingByGoodOrderId, queryEnd));
    if (ordersData == null || ordersData.isEmpty()) {
      return null;
    }

    HashSet<String> queryingKeys = new HashSet<String>();
    queryingKeys.add(key);
    List<ResultImpl> allData = new ArrayList<ResultImpl>(ordersData.size());
    for (Row orderData : ordersData.values()) {
      allData.add(createResultFromOrderData(orderData, queryingKeys));
    }

    // accumulate as Long
    try {
      boolean hasValidData = false;
      long sum = 0;
      for (ResultImpl r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsLong();
          hasValidData = true;
        }
      }
      if (hasValidData) {
        return new KV(key, Long.toString(sum));
      }
    } catch (TypeException e) {
    }

    // accumulate as double
    try {
      boolean hasValidData = false;
      double sum = 0;
      for (ResultImpl r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsDouble();
          hasValidData = true;
        }
      }
      if (hasValidData) {
        return new KV(key, Double.toString(sum));
      }
    } catch (TypeException e) {
    }

    return null;
  }
}
