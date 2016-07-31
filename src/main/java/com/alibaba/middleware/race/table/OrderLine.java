package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.RecordIndex;

public class OrderLine {

    private RecordIndex recodeIndex;
    String line;

    public OrderLine(RecordIndex recodeIndex, String line) {
        this.recodeIndex = recodeIndex;
        this.line = line;
    }

    public RecordIndex getRecodeIndex() {
        return recodeIndex;
    }

    public String getLine() {
        return line;
    }
}
