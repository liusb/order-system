package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class ResultIterator implements Iterator<OrderSystem.Result> {

    private ArrayList<ResultImpl> results;

    public ResultIterator(ArrayList<ResultImpl> results) {
        this.results = results;
        Collections.sort(results);
    }

    @Override
    public boolean hasNext() {
        return results.size() > 0;
    }

    @Override
    public OrderSystem.Result next() {
        int lastIndex = results.size()-1;
        ResultImpl nextResult = results.get(lastIndex);
        results.remove(lastIndex);
        return nextResult;
    }

    @Override
    public void remove() { }
}
