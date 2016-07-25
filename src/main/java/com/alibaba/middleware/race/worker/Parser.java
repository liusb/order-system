package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.table.HashTable;
import com.alibaba.middleware.race.table.Row;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Parser implements Runnable {
    private LinkedBlockingQueue<String> in;
    private ArrayList<LinkedBlockingQueue<Row>> outs;
    private String line;
    private HashTable table;
    private HashIndex index;
    private int rowCount;
    private long threadId;

    public Parser(LinkedBlockingQueue<String> in, ArrayList<LinkedBlockingQueue<Row>> outs,
                  HashTable table ,HashIndex index) {
        this.in = in;
        this.outs = outs;
        this.line = null;
        this.index = index;
        this.rowCount = 0;
        this.threadId = 0;
        this.table = table;
    }

    private int getOutIndex(Row row) {
        if(row.getHashCode() == 0) {
            throw new RuntimeException("row doesn't set hash code.");
        }
        return index.getFileIndex(row.getHashCode());
    }

    private Row parseRow(String line) {
        String[] kvs = line.split("\t");
        Row row = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            int columnId = this.table.getColumnId(key);
            row.insert(columnId, value);
            if (columnId == 0) {
                row.setHashCode(HashIndex.getHashCode(value));
            }
        }
        return row;
    }

    private void nextLine() {
        while (true) {
            line = null;
            try {
                line = in.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (line != null) {
                return;
            }
        }
    }

    @Override
    public void run() {
        this.threadId = Thread.currentThread().getId();
//        System.out.println("INFO: Parser thread is running. Thread id:" + threadId);
        while (true) {
            this.nextLine();
            if(line.isEmpty()) {
                break;
            }
//            Row row = parseRow(line);
//            while (true) {
//                try {
//                    outs.get(getOutIndex(row)).put(row);
//                    break;
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            rowCount ++;
//            if(rowCount % 30 == 0) {
//                System.out.println("INFO: Parser count is:" + rowCount + ". Thread id:" + threadId);
//            }
        }
        System.out.println("INFO: Parser thread completed. rowCount:" + rowCount + " Thread id:" + threadId);
    }
}
