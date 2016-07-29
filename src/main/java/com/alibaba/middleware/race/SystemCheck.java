package com.alibaba.middleware.race;

import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.BuyerTable;
import com.alibaba.middleware.race.table.GoodTable;
import com.alibaba.middleware.race.table.HashTable;
import com.alibaba.middleware.race.worker.LineReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SystemCheck {

    public static void systemCheck(Collection<String> orderFiles,
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
