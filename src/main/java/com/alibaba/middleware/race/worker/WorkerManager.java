package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.store.PageStore;
import com.alibaba.middleware.race.table.HashTable;
import com.alibaba.middleware.race.table.Row;
import com.alibaba.middleware.race.table.TableManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class WorkerManager {
    private Collection<String> storeFolders;
    private Collection<String> orderFiles;
    private Collection<String> buyerFiles;
    private Collection<String> goodFiles;

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
    }

    private void processGoodRecord() {
        int bucketBitSize = 10;
        HashTable goodTable = TableManager.instance().goodTable;
        goodTable.init(this.storeFolders, "goodid", 1<<bucketBitSize);
        HashIndex index = new HashIndex(bucketBitSize, 3);
        goodTable.setIndex(index);
        ArrayList<LinkedBlockingQueue<String>> inQueues = createQueues(4, 16);
        ArrayList<LinkedBlockingQueue<Row>> outQueues = createQueues(this.storeFolders.size(), 16);
        ArrayList<Reader> readers = createReaders(goodFiles, inQueues);
        ArrayList<Parser> parsers = createParser(inQueues, outQueues, goodTable, index);
        ArrayList<Writer> writers = createWriter(outQueues, goodTable.getPageFiles(), index);
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
        String emptyLine = "";
        for (LinkedBlockingQueue<String> queue: inQueues) {
            while (true) {
                try {
                    queue.put(emptyLine);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        waitThreads(parserThreads);
        Row emptyRow = new Row();
        for (LinkedBlockingQueue<Row> queue: outQueues) {
            while (true) {
                try {
                    queue.put(emptyRow);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        waitThreads(writerThreads);
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

}
