package com.alibaba.middleware.race;

import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.BuyerTable;
import com.alibaba.middleware.race.table.GoodTable;
import com.alibaba.middleware.race.table.HashTable;
import com.alibaba.middleware.race.table.OrderTable;
import com.alibaba.middleware.race.worker.LineReader;

import java.io.IOException;
import java.util.*;

public class TestMain {

    public static void main(String[] args) throws IOException,
            InterruptedException {

        // init order system
        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();

        orderFiles.add("./order_records.txt");
        buyerFiles.add("./buyer_records.txt");
        goodFiles.add("./good_records.txt");
//        orderFiles.add("./prerun_data/order.0.0");
//        orderFiles.add("./prerun_data/order.0.3");
//        orderFiles.add("./prerun_data/order.1.1");
//        orderFiles.add("./prerun_data/order.2.2");
//        buyerFiles.add("./prerun_data/buyer.0.0");
//        buyerFiles.add("./prerun_data/buyer.1.1");
//        goodFiles.add("./prerun_data/good.0.0");
//        goodFiles.add("./prerun_data/good.1.1");
//        goodFiles.add("./prerun_data/good.2.2");
        storeFolders.add("./1");
        storeFolders.add("./2");
        storeFolders.add("./3");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 检查构建是否有错误
        systemCheck(orderFiles, buyerFiles, goodFiles, os);

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
        OrderSystem.Result result = os.queryOrder(orderid, queryingKeys);
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
        Iterator<OrderSystem.Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
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
        OrderSystem.KeyValue sum = os.sumOrdersByGood(goodid, attr);
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

    private static void systemCheck(Collection<String> orderFiles,
                                    Collection<String> buyerFiles,
                                    Collection<String> goodFiles,
                                    OrderSystem os) {
        long totalCount = 0;
        for (PageStore pageFile: GoodTable.getInstance().baseTable.getPageFiles()) {
            totalCount += pageFile.FileCheck();
        }
        System.out.println("========================total Count:" + totalCount);
        totalCount = 0;
        for (PageStore pageFile: BuyerTable.getInstance().baseTable.getPageFiles()) {
            totalCount += pageFile.FileCheck();
        }
        System.out.println("========================total Count:" + totalCount);
        totalCount = 0;
        for (PageStore pageFile: OrderTable.getInstance().baseTable.getPageFiles()) {
            totalCount += pageFile.FileCheck();
        }
        System.out.println("========================total Count:" + totalCount);

        checkOrder(orderFiles, os);
        checkHashTable(goodFiles, GoodTable.getInstance().baseTable, "goodid");
        checkHashTable(buyerFiles, BuyerTable.getInstance().baseTable, "buyerid");
    }

    private static void checkOrder(Collection<String> orderFiles, OrderSystem os) {
        for (String file: orderFiles) {
            LineReader lineReader = new LineReader(file);
            String line;
            HashMap<String, String> raw = new HashMap<String, String>();
            OrderSystem.Result result;
            int lineCount = 0;
            while (true) {
                line = lineReader.nextLine();
                if (line == null) {
                    break;
                }
                raw.clear();
                String[] kvs = line.split("\t");
                for (String rawkv : kvs) {
                    int p = rawkv.indexOf(':');
                    String key = rawkv.substring(0, p);
                    String value = rawkv.substring(p + 1);
                    if (key.length() == 0 || value.length() == 0) {
                        throw new RuntimeException("Bad data:" + line);
                    }
                    raw.put(key, value);
                }
                result = os.queryOrder(Long.parseLong(raw.get("orderid")), null);
                for (Map.Entry<String, String> entry: raw.entrySet()) {
                    if (!result.get(entry.getKey()).valueAsString().equals(entry.getValue())) {
                        throw new RuntimeException("竟然不相等, raw:" + entry.getValue()
                                + ", find:" + result.get(entry.getKey()).valueAsString());
                    }
                }
                lineCount++;
            }
            System.out.println(file + " checked. total count: " +lineCount);
        }
    }

    private static void checkHashTable(Collection<String> rawFiles, HashTable table, String keyStr) {
        for (String file: rawFiles) {
            LineReader lineReader = new LineReader(file);
            String line;
            HashMap<String, String> raw = new HashMap<String, String>();
            HashMap<String, Object> result;
            int lineCount = 0;
            while (true) {
                line = lineReader.nextLine();
                if (line == null) {
                    break;
                }
                raw.clear();
                String[] kvs = line.split("\t");
                for (String rawkv : kvs) {
                    int p = rawkv.indexOf(':');
                    String key = rawkv.substring(0, p);
                    String value = rawkv.substring(p + 1);
                    if (key.length() == 0 || value.length() == 0) {
                        throw new RuntimeException("Bad data:" + line);
                    }
                    raw.put(key, value);
                }
                result = table.findRecord(raw.get(keyStr));
                for (Map.Entry<String, String> entry: raw.entrySet()) {
                    if (!result.get(entry.getKey()).toString().equals(entry.getValue())) {
                        throw new RuntimeException("竟然不相等, raw:" + entry.getValue()
                                + ", find:" + result.get(entry.getKey()).toString());
                    }
                }
                lineCount++;
            }
            System.out.println(file + " checked. total count: " +lineCount);
        }
    }

}
