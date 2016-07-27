package com.alibaba.middleware.race.index;

public class RowIndex {

    private final RecordIndex recodeIndex;
    private final int hashCode;

    public RowIndex(RecordIndex recodeIndex, int hashCode) {
        this.recodeIndex = recodeIndex;
        this.hashCode = hashCode;
    }

    public RecordIndex getRecodeIndex() {
        return recodeIndex;
    }

    public int getHashCode() {
        return hashCode;
    }
}
