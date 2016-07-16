package com.alibaba.middleware.race.store.fs;

import com.alibaba.middleware.race.store.PageFile;

import java.util.concurrent.ConcurrentHashMap;

public class PageFileManager {
    private static PageFileManager ourInstance = null;

    private ConcurrentHashMap<String, PageFile> pagedFiles = new ConcurrentHashMap<String, PageFile>();

    public static PageFileManager getInstance() {
        return ourInstance;
    }

    private PageFileManager() {
    }

    public static PageFileManager createInstance() {
        if(ourInstance == null) {
            synchronized (PageFileManager.class) {
                if(ourInstance == null) {
                    ourInstance = new PageFileManager();
                }
            }
        }
        return ourInstance;
    }

    public PageFile openFile(String file) {
        return pagedFiles.get(file);
    }

}
