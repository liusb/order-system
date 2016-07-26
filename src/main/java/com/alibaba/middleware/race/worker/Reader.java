package com.alibaba.middleware.race.worker;

import java.io.IOException;
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
        int step = (int)(threadId%3);
        int i;
        try {
            for (String file : this.files) {
                in = new LineReader(file);
                String line = in.readLine();
                while (line != null) {
                    while (true) {
                        try {
                            for (i=1; i < 16; i++) {
                                outs.get((int) (lineCount+i*step) % outSize).offer(line, 420, TimeUnit.MICROSECONDS);
                            }
                            if (i==16) {
                                outs.get((int) (lineCount + threadId) % outSize).put(line);
                            }
                            break;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    line = in.readLine();
                    lineCount++;
                }
                in.close();
            }
            System.out.println("INFO: Reader thread completed. lineCount:" + lineCount + " Thread id:" + threadId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
