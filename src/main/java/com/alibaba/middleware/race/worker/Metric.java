package com.alibaba.middleware.race.worker;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Metric implements Runnable {

    private TreeMap<String, LinkedBlockingQueue> queues;
    private boolean stop;
    private int sleepMills;

    public Metric() {
        this.queues = new TreeMap<String, LinkedBlockingQueue>();
        this.stop = false;
        this.sleepMills = 500;
    }

    public void setStop() {
        this.stop = true;
    }

    public void addQueue(String name, ArrayList<LinkedBlockingQueue>  queues) {
        for (int i=0; i<queues.size(); i++) {
            this.queues.put(name+i, queues.get(i));
        }
    }

    public void setSleepMills(int mills) {
        this.sleepMills = mills;
    }

    @Override
    public void run() {
//        try {
//            Thread.sleep(500);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        long beginTime = System.currentTimeMillis();
        while (!this.stop) {
            System.out.println("Metric run Millis ====>" + (System.currentTimeMillis()-beginTime));
            for (Map.Entry<String,LinkedBlockingQueue> entry : queues.entrySet()) {
                System.out.println(entry.getKey() + " size===>: " + entry.getValue().size());
            }
            try {
                Thread.sleep(this.sleepMills);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
