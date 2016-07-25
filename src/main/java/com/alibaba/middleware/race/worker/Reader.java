package com.alibaba.middleware.race.worker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Reader implements Runnable {
    private ArrayList<LinkedBlockingQueue<String>> outs;
    private int outSize;
    private Collection<String> files;
    private LineReader in;
    private long lineCount;
    private long threadId;

    public Reader(Collection<String> files, ArrayList<LinkedBlockingQueue<String>> outs) {
        this.outs = outs;
        this.outSize = outs.size();
        this.lineCount = 0;
        this.files = files;
    }

    @Override
    public void run() {
        this.threadId = Thread.currentThread().getId();
//        System.out.println("INFO: Reader thread is running. Thread id:" + threadId);
        for (String file: this.files) {
            in = new LineReader(file);
            String line = in.nextLine();
            while (line != null) {
                while (true) {
                    try {
                        if (!outs.get((int) lineCount % outSize).offer(line, 1, TimeUnit.MICROSECONDS)) {
                            outs.get((int) (lineCount + threadId) % outSize).put(line);
                        }
                        break;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                line = in.nextLine();
                lineCount++;
//            if(lineCount % 30 == 0) {
//                System.out.println("INFO: Reader count is:" + lineCount  + ". Thread id:" + threadId);
//            }
            }
            in.close();
        }
        System.out.println("INFO: Reader thread completed. lineCount:" + lineCount + " Thread id:" + threadId);
    }
}
