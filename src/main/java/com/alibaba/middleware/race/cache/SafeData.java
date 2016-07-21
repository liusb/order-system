package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.store.Data;

public class SafeData {

    private static final ThreadLocal<Data> data = new ThreadLocal<Data>() {
        protected Data initialValue() {
            Data data = new Data(new byte[1<<20]);
            return data;
        }
    };

    public static Data getData() {
        return data.get();
    }

}
