package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.store.PageStore;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class Table {
    protected HashMap<String, Column> columns;
    protected ArrayList<PageStore> storeFiles;
    protected String name;

    public Column getColumn(String name) {
        Column column = columns.get(name);
        if (column != null) {
            return column;
        } else {
            return addColumn(name);
        }
    }

    private synchronized Column addColumn(String name) {
        Column column = columns.get(name);
        if (column != null) {
            return column;
        } else {
            column = new Column(name, columns.size());
            columns.put(name, column);
            return column;
        }
    }

    public ArrayList<PageStore> getPageFiles() {
        return this.storeFiles;
    }
}
