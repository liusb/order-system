package com.alibaba.middleware.race;

import com.alibaba.middleware.race.table.BuyerTable;
import com.alibaba.middleware.race.table.GoodTable;
import com.alibaba.middleware.race.worker.LineReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SystemCheck {

    public static void systemCheck(Collection<String> orderFiles,
                                    Collection<String> buyerFiles,
                                    Collection<String> goodFiles,
                                    OrderSystem os) {
        checkOrder(orderFiles, os);
        checkGoodIndex(goodFiles);
        checkBuyerIndex(buyerFiles);
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

    private static void checkGoodIndex(Collection<String> rawFiles) {
        for (String file: rawFiles) {
            LineReader lineReader = new LineReader(file);
            String line;
            HashMap<String, String> raw = new HashMap<String, String>();
            HashMap<String, String> result;
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
                result = GoodTable.getInstance().findFromFile(raw.get("goodid"));
                if (lineCount == 251) {
                    System.out.println();
                }
                for (Map.Entry<String, String> entry: raw.entrySet()) {
                    if (!result.get(entry.getKey()).equals(entry.getValue())) {
                        throw new RuntimeException("竟然不相等, raw:" + entry.getValue()
                                + ", find:" + result.get(entry.getKey()));
                    }
                }
                lineCount++;
            }
            System.out.println(file + " checked. total count: " +lineCount);
        }
    }

    private static void checkBuyerIndex(Collection<String> rawFiles) {
        for (String file: rawFiles) {
            LineReader lineReader = new LineReader(file);
            String line;
            HashMap<String, String> raw = new HashMap<String, String>();
            HashMap<String, String> result;
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
                result = BuyerTable.getInstance().findFromFile(raw.get("buyerid"));
                for (Map.Entry<String, String> entry: raw.entrySet()) {
                    if (!result.get(entry.getKey()).equals(entry.getValue())) {
                        throw new RuntimeException("竟然不相等, raw:" + entry.getValue()
                                + ", find:" + result.get(entry.getKey()));
                    }
                }
                lineCount++;
            }
            System.out.println(file + " checked. total count: " +lineCount);
        }
    }

}
