package com.alibaba.middleware.race.store;


import com.alibaba.middleware.race.utils.Constants;

public class HashDataPage extends Page{

    public static final int PreviousPos = 5;
    public static final int NextPos = 9;

    public HashDataPage(Data data, int pageId) {
        this.data = data;
        this.setPosInFile(((long) pageId) * Constants.PAGE_SIZE);
    }

    public void writeDefaultHeader() {
        this.data.writeByte(Page.TYPE_DATA_HASH);
        this.data.writeInt(0);
        this.data.writeInt(-1);
        this.data.writeInt(-1);
    }

    public static Page read(Data data, int pageId) {
        HashDataPage p = new HashDataPage(data, pageId);
        p.read();
        return p;
    }

    private void read() {
        data.reset();
        data.readByte();
    }

    @Override
    public boolean canRemove() {
        return true;
    }
}
