package com.alibaba.middleware.race.table;

import java.util.HashMap;

public class Table {
    protected HashMap<String, Integer> columns;
    protected String name;

    public Table(String name) {
        this.name = name;
    }
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

    public void setBaseColumns(String[] columnsKeys) {
        this.columns = new HashMap<String, Integer>();
        int columnsId = columns.size();
        for (String key: columnsKeys) {
            this.columns.put(key, columnsId);
            columnsId++;
        }
    }

    public boolean containColumn(String name) {
        return this.columns.containsKey(name);
    }

}
