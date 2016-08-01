package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

public class SkipListIterator implements Iterator<OrderSystem.Result> {

    private Iterator<OrderSystem.Result> result;

    public SkipListIterator(ConcurrentSkipListSet<OrderSystem.Result> result) {
        this.result = result.iterator();

    }

    @Override
    public boolean hasNext() {
        return result.hasNext();
    }

    @Override
    public OrderSystem.Result next() {
        return result.next();
    }

    @Override
    public void remove() {

    }
}
