package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerManager implements Runnable {

    private static final int PARSER_THREAD_NUM = 24;
    private static final int IN_QUEUE_SIZE = 32;
    private static final int OUT_QUEUE_SIZE = 64;
    private static final int MetricTime = 10000;

    private Collection<String> storeFolders;
    private Collection<String> orderFiles;
    private Collection<String> buyerFiles;
    private Collection<String> goodFiles;

    private final static String emptyLine = "";
    private final static Row emptyRow = new Row();
    private final static OrderIdRowIndex emptyOrderIdRowIndex = new OrderIdRowIndex((byte)0, RowIndex.EMPTY_FLAG);
    private final static BuyerIdRowIndex emptyBuyerIdRowIndex = new BuyerIdRowIndex((byte)0, RowIndex.EMPTY_FLAG);

    public void setStoreFolders(Collection<String> storeFolders) {
        this.storeFolders = storeFolders;
    }

    public void setOrderFiles(Collection<String> orderFiles) {
        this.orderFiles = orderFiles;
    }

    public void setBuyerFiles(Collection<String> buyerFiles) {
        this.buyerFiles = buyerFiles;
    }

    public void setGoodFiles(Collection<String> goodFiles) {
        this.goodFiles = goodFiles;
    }

    @Override
    public void run() {
        long beginTime = System.currentTimeMillis();
        processGoodRecord();
        System.out.println("Good Time ====>" + (System.currentTimeMillis() - beginTime));
        processBuyerRecord();
        System.out.println("Buyer Time =====>" + (System.currentTimeMillis() - beginTime));
        processOrderRecord();
        System.out.println("Order Time =====>" + (System.currentTimeMillis() - beginTime));

        BuyerTable.getInstance().reopen();
        GoodTable.getInstance().reopen();
        OrderTable.getInstance().reopen();
    }

    private void processGoodRecord() {
        GoodTable table = GoodTable.getInstance();
        table.init(this.storeFolders);
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(table.baseTable.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<Reader> readers = createReaders(goodFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable);
        ArrayList<Writer> writers = createWriter(outQueues, table.baseTable);
        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (Reader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (Parser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }
        ArrayList<Thread> writerThreads = new ArrayList<Thread>();
        for (Writer writer: writers) {
            writerThreads.add(new Thread(writer));
        }

        Metric metric = new Metric();
        addQueueToMetric(metric, "Good inQueue ", inQueues);
        addQueueToMetric(metric, "Good outQueue ", outQueues);
        metric.setSleepMills(MetricTime);
        Thread metricThread = new Thread(metric);
        metricThread.start();

        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(writerThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        sendEndMsg(outQueues, emptyRow);
        waitThreads(writerThreads);

        metric.setStop();
    }

    private void processBuyerRecord() {
        BuyerTable table = BuyerTable.getInstance();
        table.init(this.storeFolders);
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(table.baseTable.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<Reader> readers = createReaders(buyerFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable);
        ArrayList<Writer> writers = createWriter(outQueues, table.baseTable);
        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (Reader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (Parser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }
        ArrayList<Thread> writerThreads = new ArrayList<Thread>();
        for (Writer writer: writers) {
            writerThreads.add(new Thread(writer));
        }

        Metric metric = new Metric();
        addQueueToMetric(metric, "Buyer inQueue ", inQueues);
        addQueueToMetric(metric, "Buyer outQueue ", outQueues);
        metric.setSleepMills(MetricTime);
        Thread metricThread = new Thread(metric);
        metricThread.start();

        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(writerThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        sendEndMsg(outQueues, emptyRow);
        waitThreads(writerThreads);

        metric.setStop();
    }

    private void processOrderRecord() {
        OrderTable table = OrderTable.getInstance();
        table.init(storeFolders);
        HashIndex orderIndexIndex = table.orderIndex.getIndex();
        HashIndex buyerIndexIndex = table.buyerCreateTimeIndex.getIndex();
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(table.baseTable.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexQueues
                = createQueues(table.orderIndex.getPageFiles().size(), 2*OUT_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexQueues
                = createQueues(table.buyerCreateTimeIndex.getPageFiles().size(), 2*OUT_QUEUE_SIZE);
        ArrayList<Reader> readers = createReaders(orderFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable);
        ArrayList<OrderWriter> orderWriters = createOrderWriter(outQueues, table.baseTable,
                orderIndexIndex, orderIndexQueues, buyerIndexIndex, buyerIndexQueues);
        ArrayList<IndexWriter<OrderIdRowIndex>> orderIndexWriters = createIndexWriter(orderIndexQueues,
                table.orderIndex.getPageFiles(), orderIndexIndex);
        ArrayList<IndexWriter<BuyerIdRowIndex>> buyerIndexWriters = createIndexWriter(buyerIndexQueues,
                table.buyerCreateTimeIndex.getPageFiles(), buyerIndexIndex);

        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (Reader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (Parser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }
        ArrayList<Thread> orderWriterThreads = new ArrayList<Thread>();
        for (OrderWriter writer: orderWriters) {
            orderWriterThreads.add(new Thread(writer));
        }
        ArrayList<Thread> orderIndexWriterThreads = new ArrayList<Thread>();
        for (IndexWriter writer: orderIndexWriters) {
            orderIndexWriterThreads.add(new Thread(writer));
        }
        ArrayList<Thread> buyerIndexWriterThreads = new ArrayList<Thread>();
        for (IndexWriter writer: buyerIndexWriters) {
            buyerIndexWriterThreads.add(new Thread(writer));
        }

        Metric metric = new Metric();
        addQueueToMetric(metric, "Order inQueue ", inQueues);
        addQueueToMetric(metric, "Order outQueue ", outQueues);
        addQueueToMetric(metric, "OrderIndex queue ", orderIndexQueues);
        addQueueToMetric(metric, "BuyerIndex queue ", buyerIndexQueues);
        metric.setSleepMills(MetricTime*6);
        Thread metricThread = new Thread(metric);
        metricThread.start();


        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(orderWriterThreads);
        startThreads(orderIndexWriterThreads);
        startThreads(buyerIndexWriterThreads);

        waitThreads(readerThreads);
        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        sendEndMsg(outQueues, emptyRow);
        waitThreads(orderWriterThreads);

        sendEndMsg(orderIndexQueues, emptyOrderIdRowIndex);
        waitThreads(orderIndexWriterThreads);

        sendEndMsg(buyerIndexQueues, emptyBuyerIdRowIndex);
        waitThreads(buyerIndexWriterThreads);

        metric.setStop();
    }

    private void startThreads(ArrayList<Thread> workers) {
        for (Thread worker: workers) {
            worker.start();
        }
    }

    private <T> void sendEndMsg(ArrayList<LinkedBlockingQueue<T>> queues, T msg) {
        for (LinkedBlockingQueue<T> queue: queues) {
            while (true) {
                try {
                    queue.put(msg);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void waitThreads(ArrayList<Thread> workers) {
        for (Thread thread: workers) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static <T> ArrayList<LinkedBlockingQueue<T>> createQueues(int queueCount, int capacity) {
        ArrayList<LinkedBlockingQueue<T>> queues = new ArrayList<LinkedBlockingQueue<T>>();
        for (int i = 0; i < queueCount; i++) {
            queues.add(new LinkedBlockingQueue<T>(capacity));
        }
        return queues;
    }

    private static ArrayList<Reader> createReaders(Collection<String> files,
                                                   ArrayList<LinkedBlockingQueue<String>> inQueues) {
        HashMap<String, ArrayList<String>> fileSplits = new HashMap<String, ArrayList<String>>(3);
        for (String file: files) {
            String prefix = file.substring(0, file.lastIndexOf('/'));
            if (!fileSplits.containsKey(prefix)) {
                fileSplits.put(prefix, new ArrayList<String>());
            }
            fileSplits.get(prefix).add(file);
        }
        ArrayList<Reader> readers = new ArrayList<Reader>();
        for (Map.Entry<String, ArrayList<String>> entry: fileSplits.entrySet()) {
            readers.add(new Reader(entry.getValue(), inQueues));
        }
        return readers;
    }

    private static ArrayList<Parser> createParser(ArrayList<LinkedBlockingQueue<String>> inQueues,
                                                  ArrayList<LinkedBlockingQueue<Row>> outQueues,
                                                  HashTable table) {
        ArrayList<Parser> parsers = new ArrayList<Parser>();
        for (LinkedBlockingQueue<String> queue: inQueues) {
            parsers.add(new Parser(queue, outQueues, table, table.getIndex()));
        }
        return parsers;
    }

    private static ArrayList<Writer> createWriter(ArrayList<LinkedBlockingQueue<Row>> outQueues,
                                                  HashTable table) {
        ArrayList<Writer> writers = new ArrayList<Writer>();
        for (int i=0; i< outQueues.size(); i++) {
            writers.add(new Writer(outQueues.get(i), table.getPageFiles().get(i), table.getIndex()));
        }
        return writers;
    }

    private static ArrayList<OrderWriter> createOrderWriter(
            ArrayList<LinkedBlockingQueue<Row>> outQueues, HashTable table,
            HashIndex orderIndexIndex, ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexOut,
            HashIndex buyerIndexIndex, ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexOut) {
        ArrayList<OrderWriter> writers = new ArrayList<OrderWriter>();
        int orderColumnId = table.getColumnId("orderid");
        int buyerIdColumnId = table.getColumnId("buyerid");
        int buyerCreateTimeColumnId = table.getColumnId("createtime");
        for (int i=0; i< outQueues.size(); i++) {
            writers.add(new OrderWriter(outQueues.get(i), table.getPageFiles().get(i), (byte)i, table.getIndex(),
                    orderIndexIndex, orderIndexOut, buyerIndexIndex, buyerIndexOut,
                    orderColumnId, buyerIdColumnId, buyerCreateTimeColumnId));
        }
        return writers;
    }

    private static <T extends RowIndex> ArrayList<IndexWriter<T>> createIndexWriter(
            ArrayList<LinkedBlockingQueue<T>> outQueues, ArrayList<PageStore> pageFiles, HashIndex index) {
        ArrayList<IndexWriter<T>> writers = new ArrayList<IndexWriter<T>>();
        for (int i=0; i< outQueues.size(); i++) {
            writers.add(new IndexWriter<T>(outQueues.get(i), pageFiles.get(i), index));
        }
        return writers;
    }

    private static <T> void addQueueToMetric(Metric metric, String name, ArrayList<LinkedBlockingQueue<T>> queues) {
        ArrayList<LinkedBlockingQueue>  addQueues = new ArrayList<LinkedBlockingQueue>();
        for (LinkedBlockingQueue queue: queues) {
            addQueues.add(queue);
        }
        metric.addQueue(name, addQueues);
    }
}
