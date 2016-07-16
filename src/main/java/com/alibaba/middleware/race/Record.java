package com.alibaba.middleware.race;

import java.util.HashMap;

@SuppressWarnings("serial")
public class Record extends HashMap<String, Field> {
    public Record() {
        super();
    }

    public Record(Field field) {
        super();
        this.put(field.key(), field);
    }

    public Field getKV(String key) {
        Field field = this.get(key);
        if (field == null) {
            throw new RuntimeException(key + " is not exist");
        }
        return field;
    }

    public Record putKV(String key, String value) {
        Field field = new Field(key, value);
        this.put(field.key(), field);
        return this;
    }

    public Record putKV(String key, long value) {
        Field field = new Field(key, Long.toString(value));
        this.put(field.key(), field);
        return this;
    }
}
