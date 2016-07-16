package com.alibaba.middleware.race.store;


public class HashDataPage extends Page{

    // 头部的长度，即记录存储的起始位置
    private static final int HEADER_LENGTH = 4;

    private HashDataPage(Data data, int pageId) {
        this.data = data;
        this.pageId = pageId;
    }

    public static Page read(Data data, int pageId) {
        HashDataPage p = new HashDataPage(data, pageId);
        p.read();
        p.setPos(pageId);
        return p;
    }

    private void read() {
        data.reset();
        data.readByte();
    }

    @Override
    public Page nextPage() {
        return null;
    }

    @Override
    public Page previousPage() {
        return null;
    }

    @Override
    public void write() {

    }

    @Override
    public boolean canRemove() {
        return false;
    }
}
