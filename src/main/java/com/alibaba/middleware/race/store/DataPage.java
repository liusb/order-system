package com.alibaba.middleware.race.store;


import com.alibaba.middleware.race.utils.MathUtils;

public class DataPage implements Comparable<DataPage> {

    public static final int HeaderLength = 8;
    public static final int DataLenPos = 0;
    public static final int NextPos = DataLenPos+4;
    private int dataLen;
    private int nextPage;
    private Data data;

    private int pageId;

    public DataPage(Data data, int pageId) {
        this.nextPage = -1;
        this.dataLen = 0;
        this.data = data;
        this.pageId = pageId;
        this.data.setPos(HeaderLength);
    }

    public void writeHeader() {
        this.data.setInt(DataLenPos, dataLen);
        this.data.setInt(NextPos, nextPage);
    }

    public void parseHeader() {
        int pos = this.data.getPos();
        this.data.reset();
        dataLen = this.data.readInt();
        nextPage = this.data.readInt();
        this.data.setPos(pos);
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }

    public int getDataLen() {
        return dataLen;
    }

    public void setNextPage(int nextPage) {
        this.nextPage = nextPage;
    }

    public int getNextPage() {
        return nextPage;
    }

    public int getPageId() {
        return pageId;
    }

    public Data getData() {
        return data;
    }

    @Override
    public int compareTo(DataPage other) {
        return MathUtils.compareInt(pageId, other.pageId);
    }
}
