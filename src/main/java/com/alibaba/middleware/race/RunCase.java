package com.alibaba.middleware.race;

import com.alibaba.middleware.race.worker.LineReader;

import java.io.IOException;
import java.util.*;

public class RunCase {

    public static void main(String[] args) throws IOException,
            InterruptedException {

        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();

        orderFiles.add("./prerun_data/order.0.0");
        orderFiles.add("./prerun_data/order.0.3");
        orderFiles.add("./prerun_data/order.1.1");
        orderFiles.add("./prerun_data/order.2.2");
        buyerFiles.add("./prerun_data/buyer.0.0");
        buyerFiles.add("./prerun_data/buyer.1.1");
        goodFiles.add("./prerun_data/good.0.0");
        goodFiles.add("./prerun_data/good.1.1");
        goodFiles.add("./prerun_data/good.2.2");
        storeFolders.add("./prerun_data/1");
        storeFolders.add("./prerun_data/2");
        storeFolders.add("./prerun_data/3");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 检查构建是否有错误
        //SysteCheck.systemCheck(orderFiles, buyerFiles, goodFiles, os);

        // 用例
        String line;

        long orderId;
        Collection<String> keys;
        String keysStr;

        long startTime;
        long endTime;
        String buyerid;

        String salerid;
        String goodid;
        // Collection<String> keys;

        // String goodid;
        String key;

        OrderSystem.Result result;
        String resultStr;
        OrderSystem.KeyValue keyValue;
        Iterator<OrderSystem.Result> resultIterator;
        for (int i=0; i<20; i++) {
            LineReader lineReader = new LineReader("./prerun_data/case/case.1."+i);
            System.out.println("正在评测的文件为：case.0." +i);
            while (true) {
                line = lineReader.nextLine();
                if (line == null) {
                    System.out.print("Good Job.");
                    break;
                }
                if ("CASE:QUERY_ORDER".equals(line)) {
                    line = lineReader.nextLine();
                    orderId = Long.parseLong(line.substring(line.indexOf(':') + 1));
                    keysStr = lineReader.nextLine();
                    keys = parseKeys(keysStr);
                    lineReader.nextLine();
                    result = os.queryOrder(orderId, keys);
                    if (result != null) {
                        line = lineReader.nextLine();
                        resultStr = result.toString();
                        if (!line.equals(resultStr)) {
                            if (line.length() != resultStr.length() || !compareResult(line, result)) {
                                throw new RuntimeException("CASE:QUERY_ORDER, orderId:" + orderId
                                        + " " + keysStr + " 结果不一致\n"
                                        + line + "\n not equal \n" + result);
                            }
                        }
                    }
                    line = lineReader.nextLine();
                    if (result == null && !line.equals("}")) {
                        throw new RuntimeException("CASE:QUERY_ORDER, orderId:" + orderId
                                + " " + keysStr + " 程序查询返回null，实际则是：\n" + line);
                    }
                    lineReader.nextLine();
                } else if ("CASE:QUERY_BUYER_TSRANGE".endsWith(line)) {
                    line = lineReader.nextLine();
                    buyerid = line.substring(line.indexOf(':') + 1);
                    line = lineReader.nextLine();
                    startTime = Long.parseLong(line.substring(line.indexOf(':') + 1));
                    line = lineReader.nextLine();
                    endTime = Long.parseLong(line.substring(line.indexOf(':') + 1));
                    lineReader.nextLine();
                    resultIterator = os.queryOrdersByBuyer(startTime, endTime, buyerid);
                    while (resultIterator.hasNext()) {
                        line = lineReader.nextLine();
                        result = resultIterator.next();
                        resultStr = result.toString();
                        if (!line.equals(resultStr)) {
                            if (line.length() != resultStr.length() || !compareResult(line, result)) {
                                throw new RuntimeException("CASE:QUERY_BUYER_TSRANGE, buyerId:" + buyerid
                                        + " startTime:" + startTime + " endTime:" + endTime + " 结果不一致\n"
                                        + line + "\n not equal \n" + result);
                            }
                        }
                    }
                    line = lineReader.nextLine();
                    if (!line.equals("}")) {
                        throw new RuntimeException("CASE:QUERY_BUYER_TSRANGE, buyerId:" + buyerid
                                + " startTime:" + startTime + " endTime:" + endTime + "结果长度不一致");
                    }
                    lineReader.nextLine();
                } else if ("CASE:QUERY_SALER_GOOD".endsWith(line)) {
                    line = lineReader.nextLine();
                    salerid = line.substring(line.indexOf(':') + 1);
                    line = lineReader.nextLine();
                    goodid = line.substring(line.indexOf(':') + 1);
                    keysStr = lineReader.nextLine();
                    keys = parseKeys(keysStr);
                    lineReader.nextLine();
                    resultIterator = os.queryOrdersBySaler(salerid, goodid, keys);
                    while (resultIterator.hasNext()) {
                        line = lineReader.nextLine();
                        result = resultIterator.next();
                        resultStr = result.toString();
                        if (!line.equals(resultStr) && (line.length() != resultStr.length() || !compareResult(line, result))) {
                            throw new RuntimeException("CASE:QUERY_SALER_GOOD, salerid:" + salerid
                                    + " goodid:" + goodid + " " + keysStr + " 结果不一致\n"
                                    + line + "\n not equal \n" + result);
                        }
                    }
                    line = lineReader.nextLine();
                    if (!line.equals("}")) {
                        throw new RuntimeException("CASE:QUERY_BUYER_TSRANGE, salerid:" + salerid
                                + " goodid:" + goodid + " " + keysStr + "结果长度不一致");
                    }
                    lineReader.nextLine();
                } else if ("CASE:QUERY_GOOD_SUM".endsWith(line)) {
                    line = lineReader.nextLine();
                    goodid = line.substring(line.indexOf(':') + 1);
                    line = lineReader.nextLine();
                    key = line.substring(line.indexOf('[') + 1, line.indexOf(']') - 1);
                    keyValue = os.sumOrdersByGood(goodid, key);
                    line = lineReader.nextLine();
                    if ((keyValue != null && !compareSum(line.substring(line.indexOf(':') + 1), keyValue.valueAsString()))
                            || (keyValue == null && !line.equals("RESULT:null"))) {
                        throw new RuntimeException(line + "\n not equal \n" + keyValue);
                    }
                    lineReader.nextLine();
                }
            }
            lineReader.close();
        }
    }

    private static Collection<String> parseKeys(String line) {
        Collection<String> keys = new ArrayList<String>();
        if (line.equals("KEYS:[*,]")) {
            return null;
        } else if (line.equals("KEYS:[]")) {
            return keys;
        }
        String[] rawKeys = line.substring(line.indexOf('[')+1, line.indexOf(']')-1).split(",");
        keys.addAll(Arrays.asList(rawKeys));
        return keys;
    }

    private static boolean compareSum(String rawResult, String findResult) {
        try {
            long rawLong = Long.parseLong(rawResult);
            long findLong;
            try {
                findLong = Long.parseLong(findResult);
                return  rawLong == findLong;
            } catch (NumberFormatException e) {
                return false;
            }
        } catch (NumberFormatException e2) {
            try {
                double rawDouble = Double.parseDouble(rawResult);
                double findDouble;
                try {
                    findDouble = Double.parseDouble(findResult);
                    return Math.abs(findDouble - rawDouble) < 0.00001;
                } catch (NumberFormatException e) {
                    return false;
                }
            } catch (NumberFormatException e3) {
                return false;
            }
        }
    }

    private static boolean compareResult(String line, OrderSystem.Result result) {
        OrderSystem.KeyValue[] keyValues = result.getAll();
        for (OrderSystem.KeyValue keyValue: keyValues) {
            if (!line.contains(keyValue.toString())) {
                return false;
            }
        }
        return true;
    }

}
