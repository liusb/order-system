package com.alibaba.middleware.race.worker;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Reader implements Runnable {
    private ArrayList<LinkedBlockingQueue<String>> outs;
    private int outSize;
    private LineReader in;
    private long lineCount;
    private long threadId;

    public Reader(String file, ArrayList<LinkedBlockingQueue<String>> outs) {
        this.outs = outs;
        this.outSize = outs.size();
        this.lineCount = 0;
        this.in = new LineReader(file);
    }

    @Override
    public void run() {
        this.threadId = Thread.currentThread().getId();
//        System.out.println("INFO: Reader thread is running. Thread id:" + threadId);
        String line = in.nextLine();
        while (line != null) {
            while (true) {
                try {
                    outs.get((int)lineCount%outSize).put(line);
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
        System.out.println("INFO: Reader thread completed. lineCount:" + lineCount + " Thread id:" + threadId);
        in.close();
    }
}
