package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.cache.IndexCache;
import com.alibaba.middleware.race.index.*;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerManager implements Runnable {

    private static final int ORDER_READER_THREAD_NUM = 9;
    private static final int PARSER_THREAD_NUM = 20;
    private static final int IN_QUEUE_SIZE = 1024;
    private static final int OUT_QUEUE_SIZE = 16*1024;
    private static final int MetricTime = 1000;

    private Collection<String> storeFolders;
    private Collection<String> orderFiles;
    private Collection<String> buyerFiles;
    private Collection<String> goodFiles;

    private final static OffsetLine emptyLine = new OffsetLine(null, "");
    private final static GoodIdRowIndex emptyGoodIdRowIndex
            = new GoodIdRowIndex(new RecordIndex((byte)0, -1), 0, "");
    private final static OrderIdRowIndex emptyOrderIdRowIndex
            = new OrderIdRowIndex(new RecordIndex((byte)0, -1), 0, 0);
    private final static BuyerIdRowIndex emptyBuyerIdRowIndex
            = new BuyerIdRowIndex(new RecordIndex((byte)0, -1), 0, "", 0);

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
        processOrderRecord();
        System.out.println("Order Time =====>" + (System.currentTimeMillis() - beginTime));
        processGoodRecord();
        System.out.println("Good Time ====>" + (System.currentTimeMillis() - beginTime));
        processBuyerRecord();
        System.out.println("Buyer Time =====>" + (System.currentTimeMillis() - beginTime));

        GoodTable.getInstance().reopen();
        BuyerTable.getInstance().reopen();
        OrderTable.getInstance().reopen();
    }

    private void processGoodRecord() {
        GoodTable table = GoodTable.getInstance();
        table.init(this.storeFolders, this.goodFiles);
        ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<OffsetReader> readers = createOffsetReaders(table.sortGoodFiles, table.goodFilesMap.size(),
                inQueues, table.goodFilesMap.size());
        ArrayList<OffsetParser> parsers = createOffsetParser(inQueues, table.indexCache, table.baseTable);
        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (OffsetReader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (OffsetParser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }

        Metric metric = new Metric();
        addQueueToMetric(metric, "Good inQueue ", inQueues);
        metric.setSleepMills(MetricTime);
        Thread metricThread = new Thread(metric);
        metricThread.start();

        startThreads(readerThreads);
        startThreads(parserThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        metric.setStop();
    }

    private void processBuyerRecord() {
        BuyerTable table = BuyerTable.getInstance();
        table.init(this.storeFolders, this.buyerFiles);
        ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<OffsetReader> readers = createOffsetReaders(table.sortBuyerFiles, table.buyerFilesMap.size(),
                inQueues, table.buyerFilesMap.size());
        ArrayList<OffsetParser> parsers = createOffsetParser(inQueues, table.indexCache, table.baseTable);
        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (OffsetReader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (OffsetParser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }

        Metric metric = new Metric();
        addQueueToMetric(metric, "Buyer inQueue ", inQueues);
        metric.setSleepMills(MetricTime);
        Thread metricThread = new Thread(metric);
        metricThread.start();

        startThreads(readerThreads);
        startThreads(parserThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        metric.setStop();
    }

    private void processOrderRecord() {
        OrderTable table = OrderTable.getInstance();
        table.init(storeFolders, orderFiles);
        HashIndex goodIndexIndex = table.goodIndex.getIndex();
        HashIndex orderIndexIndex = table.orderIndex.getIndex();
        HashIndex buyerIndexIndex = table.buyerIndex.getIndex();
        ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues = createQueues(PARSER_THREAD_NUM, IN_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<GoodIdRowIndex>> goodIndexQueues
                = createQueues(table.goodIndex.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexQueues
                = createQueues(table.orderIndex.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexQueues
                = createQueues(table.buyerIndex.getPageFiles().size(), OUT_QUEUE_SIZE);
        ArrayList<OffsetReader> readers = createOffsetReaders(table.sortOrderFiles, table.orderFilesMap.size(),
                inQueues, table.orderFilesMap.size());
        ArrayList<OrderParser> parsers = createOrderParser(inQueues, goodIndexQueues, orderIndexQueues, buyerIndexQueues,
                goodIndexIndex, orderIndexIndex, buyerIndexIndex);
        ArrayList<IndexWriter<GoodIdRowIndex>> goodIndexWriters = createIndexWriter(goodIndexQueues,
                table.goodIndex.getPageFiles(), goodIndexIndex);
        ArrayList<IndexWriter<OrderIdRowIndex>> orderIndexWriters = createIndexWriter(orderIndexQueues,
                table.orderIndex.getPageFiles(), orderIndexIndex);
        ArrayList<IndexWriter<BuyerIdRowIndex>> buyerIndexWriters = createIndexWriter(buyerIndexQueues,
                table.buyerIndex.getPageFiles(), buyerIndexIndex);

        ArrayList<Thread> readerThreads = new ArrayList<Thread>();
        for (OffsetReader reader: readers) {
            readerThreads.add(new Thread(reader));
        }
        ArrayList<Thread> parserThreads = new ArrayList<Thread>();
        for (OrderParser parser: parsers) {
            parserThreads.add(new Thread(parser));
        }
        ArrayList<Thread> goodIndexWriterThreads = new ArrayList<Thread>();
        for (IndexWriter writer: goodIndexWriters) {
            goodIndexWriterThreads.add(new Thread(writer));
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
        addQueueToMetric(metric, "GoodIndex queue ", goodIndexQueues);
        addQueueToMetric(metric, "OrderIndex queue ", orderIndexQueues);
        addQueueToMetric(metric, "BuyerIndex queue ", buyerIndexQueues);
        metric.setSleepMills(MetricTime*6);
        Thread metricThread = new Thread(metric);
        metricThread.start();


        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(goodIndexWriterThreads);
        startThreads(orderIndexWriterThreads);
        startThreads(buyerIndexWriterThreads);

        waitThreads(readerThreads);
        sendEndMsg(inQueues, new OffsetLine(null, ""));
        waitThreads(parserThreads);

        sendEndMsg(goodIndexQueues, emptyGoodIdRowIndex);
        waitThreads(goodIndexWriterThreads);

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

    private static ArrayList<OffsetParser> createOffsetParser(ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues,
                                                       IndexCache indexCache, HashTable table) {
        ArrayList<OffsetParser> parsers = new ArrayList<OffsetParser>();
        for (LinkedBlockingQueue<OffsetLine> queue: inQueues) {
            parsers.add(new OffsetParser(queue, indexCache, table));
        }
        return parsers;
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

    private static ArrayList<OffsetReader> createOffsetReaders(String[] sortedFiles, int size,
                                                         ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues,
                                                         int readThreadNum) {
        ArrayList<HashMap<String, Byte>> fileSplits = new ArrayList<HashMap<String, Byte>>(readThreadNum);
        for (int i=0; i<readThreadNum; i++) {
            fileSplits.add(new HashMap<String, Byte>());
        }
        for (int i=0; i<size; i++) {
            fileSplits.get(i%readThreadNum).put(sortedFiles[i], (byte)i);
        }
        ArrayList<OffsetReader> readers = new ArrayList<OffsetReader>();
        for (HashMap<String, Byte> spilt: fileSplits) {
            readers.add(new OffsetReader(spilt, inQueues));
        }
        return readers;
    }

    private static ArrayList<OrderParser> createOrderParser(ArrayList<LinkedBlockingQueue<OffsetLine>> inQueues,
                                                       ArrayList<LinkedBlockingQueue<GoodIdRowIndex>> goodOIndexQueues,
                                                       ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexQueues,
                                                       ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexQueues,
                                                       HashIndex goodIndexIndex, HashIndex orderIndexIndex,
                                                       HashIndex buyerIndexIndex) {
        ArrayList<OrderParser> parsers = new ArrayList<OrderParser>();
        for (LinkedBlockingQueue<OffsetLine> queue: inQueues) {
            parsers.add(new OrderParser(queue, goodOIndexQueues, orderIndexQueues,buyerIndexQueues,
                    goodIndexIndex, orderIndexIndex, buyerIndexIndex));
        }
        return parsers;
    }
}
