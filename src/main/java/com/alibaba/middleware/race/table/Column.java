package com.alibaba.middleware.race.table;

public class Column {
    private final String name;
    private final int columnId;
    private int type;

    public Column(String name, int columnId) {
        this.name = name;
        this.columnId = columnId;
        this.type = 0;
    }

    public String getName() {
        return name;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public int getColumnId() {
        return columnId;
    }
}
