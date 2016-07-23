package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.store.PageStore;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Table {
    protected HashMap<String, Integer> columns;
    protected ArrayList<PageStore> storeFiles;
    protected String name;

    public int getColumnId(String name) {
        Integer columnId = columns.get(name);
        if (columnId != null) {
            return columnId;
        } else {
            return addColumn(name);
        }
    }

    private synchronized int addColumn(String name) {
        Integer columnId = columns.get(name);
        if (columnId != null) {
            return columnId;
        } else {
            columnId = columns.size();
            columns.put(name, columnId);
            return columnId;
        }
    }

    public ArrayList<PageStore> getPageFiles() {
        return this.storeFiles;
    }
}
