package com.alibaba.middleware.race.table;

import com.alibaba.middleware.race.index.RecordIndex;

public class OffsetLine {

    private RecordIndex recodeIndex;
    String line;

    public OffsetLine(RecordIndex recodeIndex, String line) {
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
