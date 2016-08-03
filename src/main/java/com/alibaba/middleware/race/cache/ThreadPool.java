package com.alibaba.middleware.race.cache;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    private static ThreadPool ourInstance = new ThreadPool();

    public static ThreadPool getInstance() {
        return ourInstance;
    }

    public ThreadPoolExecutor pool;

    private ThreadPool() {
         this.pool = new ThreadPoolExecutor(800, 800, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(8192));
        pool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
