package com.alibaba.middleware.race.store;


public class HashDataPage extends Page {

    public static final int HeaderLength = 12;
    public static final int PreviousPos = DataLenPos+4;
    public static final int NextPos = PreviousPos+4;

    private int previousPage;
    private int nextPage;

    public HashDataPage(Data data, int pageId) {
        this.data = data;
        this.dataLen = HeaderLength;
        this.previousPage = -1;
        this.nextPage = -1;
        this.data.reset();
        this.setPos(pageId);
    }

    public void writeHeader() {
        this.data.setInt(DataLenPos, dataLen);
        this.data.setInt(PreviousPos, previousPage);
        this.data.setInt(NextPos, nextPage);
    }

    public void readHeader() {
        dataLen = this.data.readInt();
        previousPage = this.data.readInt();
        nextPage = this.data.readInt();
    }

    public void setPreviousPage(int previousPage) {
        this.previousPage = previousPage;
    }

    public void setNextPage(int nextPage) {
        this.nextPage = nextPage;
    }

    public int getNextPage() {
        return nextPage;
    }

    public void freeData() {
        this.data.reset();
        this.data = null;
    }

    public boolean dataIsFree() {
        return this.data == null;
    }

    @Override
    public boolean canRemove() {
        return dataIsFree();
    }
}
