package com.alibaba.middleware.race.store.fs;

import com.alibaba.middleware.race.demo.Field;
import com.alibaba.middleware.race.demo.Record;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;


public abstract class RawFileHandler {

    public abstract void handleRow(Record record);

    public void handle(Collection<String> files) throws IOException {
        for (String file : files) {
            BufferedReader bfr = createReader(file);
            try {
                String line = bfr.readLine();
                while (line != null) {
                    Record kvMap = createKVMapFromLine(line);
                    handleRow(kvMap);
                    line = bfr.readLine();
                }
            } finally {
                bfr.close();
            }
        }
    }

    private BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }

    private Record createKVMapFromLine(String line) {
        String[] kvs = line.split("\t");
        Record kvMap = new Record();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            Field field = new Field(key, value);
            kvMap.put(field.key(), field);
        }
        return kvMap;
    }
}