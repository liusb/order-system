package com.alibaba.middleware.race.cache;

public class CacheHead extends CacheObject {

    @Override
    public int getMemory() {
        return 0;
    }

    @Override
    public boolean canRemove() {
        return false;
    }
}
