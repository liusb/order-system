package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.HashIndex;
import com.alibaba.middleware.race.table.HashTable;
import com.alibaba.middleware.race.table.Row;

import java.util.ArrayList;
import java.util.StringTokenizer;
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
        StringTokenizer tokenizer = new StringTokenizer(line, ":\t");
        Row row = new Row();
        String key;
        String value;
        while (tokenizer.hasMoreTokens()) {
            key = tokenizer.nextToken();
            value = tokenizer.nextToken();
            int columnId = this.table.getColumnId(key);
            if (key.equals("orderid") || key.equals("createtime")) {
                row.insert(columnId, Long.parseLong(value));
            } else {
                row.insert(columnId, value);
            }
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
        while (true) {
            this.nextLine();
            if(line.isEmpty()) {
                break;
            }
            Row row = parseRow(line);
            while (true) {
                try {
                    outs.get(getOutIndex(row)).put(row);
                    break;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            rowCount ++;
        }
        System.out.println("INFO: Parser thread completed. rowCount:" + rowCount + " Thread id:" + threadId);
    }
}
