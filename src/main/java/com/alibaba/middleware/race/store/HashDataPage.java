package com.alibaba.middleware.race.store;


public class HashDataPage extends Page {

    public static final int HeaderLength = 8;
    public static final int NextPos = DataLenPos+4;

    private int nextPage;

    public HashDataPage(Data data, int pageId) {
        super(data, HeaderLength);
        this.nextPage = -1;
        this.setPos(pageId);
    }

    public void writeHeader() {
        this.data.setInt(DataLenPos, dataLen);
        this.data.setInt(NextPos, nextPage);
    }

    public void readHeader() {
        int pos = this.data.getPos();
        this.data.reset();
        dataLen = this.data.readInt();
        nextPage = this.data.readInt();
        this.data.setPos(pos);
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
        return !dataIsFree();
    }
}
