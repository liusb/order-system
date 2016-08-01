package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.RecordIndex;

public class OffsetLine {

    private RecordIndex recodeIndex;
    private int len;
    String line;

    public OffsetLine(RecordIndex recodeIndex, String line, int len) {
        this.recodeIndex = recodeIndex;
        this.line = line;
        this.len = len;
    }

    public RecordIndex getRecodeIndex() {
        return recodeIndex;
    }

    public String getLine() {
        return line;
    }

    public int getLen() {
        return len;
    }
}
