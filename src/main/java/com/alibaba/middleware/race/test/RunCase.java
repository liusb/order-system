package com.alibaba.middleware.race.test;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.test.SystemCheck;
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

        orderFiles.add("./prerun_data/disk1/order.0.0");
        orderFiles.add("./prerun_data/disk1/order.0.3");
        orderFiles.add("./prerun_data/disk2/order.1.1");
        orderFiles.add("./prerun_data/disk3/order.2.2");
        buyerFiles.add("./prerun_data/disk1/buyer.0.0");
        buyerFiles.add("./prerun_data/disk2/buyer.1.1");
        goodFiles.add("./prerun_data/disk1/good.0.0");
        goodFiles.add("./prerun_data/disk2/good.1.1");
        goodFiles.add("./prerun_data/disk3/good.2.2");
        storeFolders.add("./prerun_data/1");
        storeFolders.add("./prerun_data/2");
        storeFolders.add("./prerun_data/3");

        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 检查构建是否有错误
        SystemCheck.systemCheck(orderFiles, buyerFiles, goodFiles, os);

        // 用例
        long beginTime = System.currentTimeMillis();
        checkCase(os, 20);
        System.out.println("Check all case used " + (System.currentTimeMillis()-beginTime) + " millis");

    }

    private static void checkCase(OrderSystem os, int caseFileLimit) {
        String line;
        long orderId;
        Collection<String> keys;
        String keysStr;
        long startTime;
        long endTime;
        String buyerid;
        String salerid;
        String goodid;
        String key;

        OrderSystem.Result result;
        String resultStr;
        OrderSystem.KeyValue keyValue;
        Iterator<OrderSystem.Result> resultIterator;
        long queryByOrderTime = 0, orderCount=0;
        long queryByBuyerTime = 0, buyerCount=0;
        long queryByGoodTime = 0, goodCount=0;
        long queryBySumTime = 0, sumCount=0;
        for (int i=0; i<caseFileLimit; i++) {
            LineReader lineReader = new LineReader("./prerun_data/case/case.1."+i);
            System.out.print("正在评测的文件为：case.1." + i);
            while (true) {
                line = lineReader.nextLine();
                if (line == null) {
                    System.out.println(" Good Job.");
                    break;
                }
                if ("CASE:QUERY_ORDER".equals(line)) {
                    line = lineReader.nextLine();
                    orderId = Long.parseLong(line.substring(line.indexOf(':') + 1));
                    keysStr = lineReader.nextLine();
                    keys = parseKeys(keysStr);
                    lineReader.nextLine();
                    long begin = System.currentTimeMillis();
                    result = os.queryOrder(orderId, keys);
                    queryByOrderTime += (System.currentTimeMillis()-begin);
                    orderCount++;
                    if (result != null) {
                        line = lineReader.nextLine();
                        resultStr = result.toString();
                        if (!line.equals(resultStr)) {
                            if (line.length() != resultStr.length() || !compareResult(line, result)) {
                                System.out.println("=======>CASE:QUERY_ORDER, orderId:" + orderId
                                        + " " + keysStr + " 结果不一致\n"
                                        + line + "\n not equal result \n" + result);
                            }
                        }
                    }
                    line = lineReader.nextLine();
                    if (result == null && !line.equals("}")) {
                        throw new RuntimeException("=======>CASE:QUERY_ORDER, orderId:" + orderId
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
                    long begin = System.currentTimeMillis();
                    resultIterator = os.queryOrdersByBuyer(startTime, endTime, buyerid);
                    queryByBuyerTime += (System.currentTimeMillis()-begin);
                    buyerCount++;
                    while (resultIterator.hasNext()) {
                        line = lineReader.nextLine();
                        result = resultIterator.next();
                        resultStr = result.toString();
                        if (!line.equals(resultStr)) {
                            if (line.length() != resultStr.length() || !compareResult(line, result)) {
                                System.out.println("=======>CASE:QUERY_BUYER_TSRANGE, buyerId:" + buyerid
                                        + " startTime:" + startTime + " endTime:" + endTime + " 结果不一致\n"
                                        + line + "\n not equal \n" + result);
                            }
                        }
                    }
                    line = lineReader.nextLine();
                    if (!line.equals("}")) {
                        throw new RuntimeException("=======>CASE:QUERY_BUYER_TSRANGE, buyerId:" + buyerid
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
                    long begin = System.currentTimeMillis();
                    resultIterator = os.queryOrdersBySaler(salerid, goodid, keys);
                    queryByGoodTime += (System.currentTimeMillis()-begin);
                    goodCount++;
                    while (resultIterator.hasNext()) {
                        line = lineReader.nextLine();
                        result = resultIterator.next();
                        resultStr = result.toString();
                        if (!line.equals(resultStr) &&
                                (line.length() != resultStr.length() || !compareResult(line, result))) {
                            System.out.println("=======>CASE:QUERY_SALER_GOOD, salerid:" + salerid
                                    + " goodid:" + goodid + " " + keysStr + " 结果不一致\n"
                                    + line + "\n not equal \n" + result);
                        }
                    }
                    line = lineReader.nextLine();
                    if (!line.equals("}")) {
                        throw new RuntimeException("=======>CASE:QUERY_BUYER_TSRANGE, salerid:" + salerid
                                + " goodid:" + goodid + " " + keysStr + "结果长度不一致");
                    }
                    lineReader.nextLine();
                } else if ("CASE:QUERY_GOOD_SUM".endsWith(line)) {
                    line = lineReader.nextLine();
                    goodid = line.substring(line.indexOf(':') + 1);
                    line = lineReader.nextLine();
                    key = line.substring(line.indexOf('[') + 1, line.indexOf(']') - 1);
                    long begin = System.currentTimeMillis();
                    keyValue = os.sumOrdersByGood(goodid, key);
                    queryBySumTime += (System.currentTimeMillis()-begin);
                    sumCount++;
                    line = lineReader.nextLine();
                    if ((keyValue != null &&
                            !compareSum(line.substring(line.indexOf(':') + 1), keyValue.valueAsString()))
                            || (keyValue == null && !line.equals("RESULT:null"))) {
                        System.out.println("=======>CASE:QUERY_GOOD_SUM" + line + "\n not equal \n" + keyValue);
                    }
                    lineReader.nextLine();
                }
            }
            lineReader.close();
        }
        System.out.println("queryByOrderTime: " + queryByOrderTime + " count:"
                + orderCount + " avg:" + queryByOrderTime*0.1/orderCount);
        System.out.println("queryByBuyerTime: " + queryByBuyerTime +  " count:"
                + buyerCount + " avg:" + queryByBuyerTime*0.1/buyerCount);
        System.out.println("queryByGoodTime: " + queryByGoodTime+ " count:"
                + goodCount + " avg:" + queryByGoodTime*0.1/goodCount);
        System.out.println("queryBySumTime: " + queryBySumTime+ " count:"
                + sumCount + " avg:" + queryBySumTime*0.1/sumCount);
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
