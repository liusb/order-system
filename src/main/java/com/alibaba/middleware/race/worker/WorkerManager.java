package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.BuyerIdRowIndex;
import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.index.OrderIdRowIndex;
import com.alibaba.middleware.race.index.RowIndex;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerManager {
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

    public void run() {
        processGoodRecord();
        processBuyerRecord();
        processOrderRecord();

        OrderTable.getInstance().reopen();
        BuyerTable.getInstance().reopen();
        GoodTable.getInstance().reopen();
    }

    private void processGoodRecord() {
        GoodTable table = GoodTable.getInstance();
        table.init(this.storeFolders);
        HashIndex index = table.baseTable.getIndex();
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(4, 16);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(this.storeFolders.size(), 16);
        ArrayList<Reader> readers = createReaders(goodFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable, index);
        ArrayList<Writer> writers = createWriter(outQueues, table.baseTable.getPageFiles(), index);
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
        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(writerThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        sendEndMsg(outQueues, emptyRow);
        waitThreads(writerThreads);
    }

    private void processBuyerRecord() {
        BuyerTable table = BuyerTable.getInstance();
        table.init(this.storeFolders);
        HashIndex index = table.baseTable.getIndex();
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(4, 16);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(this.storeFolders.size(), 16);
        ArrayList<Reader> readers = createReaders(buyerFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable, index);
        ArrayList<Writer> writers = createWriter(outQueues, table.baseTable.getPageFiles(), index);
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
        startThreads(readerThreads);
        startThreads(parserThreads);
        startThreads(writerThreads);
        waitThreads(readerThreads);

        sendEndMsg(inQueues, emptyLine);
        waitThreads(parserThreads);

        sendEndMsg(outQueues, emptyRow);
        waitThreads(writerThreads);
    }

    private void processOrderRecord() {
        OrderTable table = OrderTable.getInstance();
        table.init(storeFolders);
        HashIndex baseIndex = table.baseTable.getIndex();
        HashIndex orderIndexIndex = table.orderIndex.getIndex();
        HashIndex buyerIndexIndex = table.buyerCreateTimeIndex.getIndex();
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(4, 16);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(4, 16);
        ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexQueues = createQueues(3, 32);
        ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexQueues = createQueues(3, 32);
        ArrayList<Reader> readers = createReaders(orderFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, table.baseTable, baseIndex);
        ArrayList<OrderWriter> orderWriters = createOrderWriter(outQueues, table.baseTable.getPageFiles(),
                baseIndex, orderIndexIndex, orderIndexQueues, buyerIndexIndex, buyerIndexQueues);
        ArrayList<IndexWriter<OrderIdRowIndex>> orderIndexWriters = createIndexWriter(orderIndexQueues,
                table.orderIndex.getPageFiles(), baseIndex);
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

    }

    public static void main(String[] args) {
        WorkerManager manager = new WorkerManager();
        ArrayList<String> storeFolders = new ArrayList<String>();
        storeFolders.add("./1");
        storeFolders.add("./2");
        storeFolders.add("./3");
        manager.setStoreFolders(storeFolders);

        ArrayList<String> buyerFiles = new ArrayList<String>();
        buyerFiles.add("./buyer_records.txt");
        manager.setBuyerFiles(buyerFiles);

        ArrayList<String> goodFiles = new ArrayList<String>();
        goodFiles.add("./good_records.txt");
        manager.setGoodFiles(goodFiles);

        ArrayList<String> orderFiles = new ArrayList<String>();
        orderFiles.add("./order_records.txt");
        manager.setOrderFiles(orderFiles);

        manager.run();
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
        ArrayList<Reader> readers = new ArrayList<Reader>();
        for (String file: files) {
            readers.add(new Reader(file, inQueues));
        }
        return readers;
    }

    private static ArrayList<Parser> createParser(ArrayList<LinkedBlockingQueue<String>> inQueues,
                                                  ArrayList<LinkedBlockingQueue<Row>> outQueues,
                                                  HashTable table,HashIndex index) {
        ArrayList<Parser> parsers = new ArrayList<Parser>();
        for (LinkedBlockingQueue<String> queue: inQueues) {
            parsers.add(new Parser(queue, outQueues, table, index));
        }
        return parsers;
    }

    private static ArrayList<Writer> createWriter(ArrayList<LinkedBlockingQueue<Row>> outQueues,
                                                  ArrayList<PageStore> pageFiles, HashIndex index) {
        ArrayList<Writer> writers = new ArrayList<Writer>();
        for (int i=0; i< outQueues.size(); i++) {
            writers.add(new Writer(outQueues.get(i), pageFiles.get(i), index));
        }
        return writers;
    }

    private static ArrayList<OrderWriter> createOrderWriter(
            ArrayList<LinkedBlockingQueue<Row>> outQueues, ArrayList<PageStore> pageFiles, HashIndex index,
            HashIndex orderIndexIndex, ArrayList<LinkedBlockingQueue<OrderIdRowIndex>> orderIndexOut,
            HashIndex buyerIndexIndex, ArrayList<LinkedBlockingQueue<BuyerIdRowIndex>> buyerIndexOut) {
        ArrayList<OrderWriter> writers = new ArrayList<OrderWriter>();
        for (int i=0; i< outQueues.size(); i++) {
            writers.add(new OrderWriter(outQueues.get(i), pageFiles.get(i), (byte)i, index,
                    orderIndexIndex, orderIndexOut, buyerIndexIndex, buyerIndexOut));
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
}
