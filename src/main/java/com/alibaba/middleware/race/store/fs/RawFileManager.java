package com.alibaba.middleware.race.store.fs;

import com.alibaba.middleware.race.worker.LineReader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

public class RawFileManager {

    private static LinkedBlockingDeque<String> goodRawRecord = new LinkedBlockingDeque<String>();

    public static void readGoodFile(Collection<String> files) {
        long count = 0;
        for (String file: files) {
            LineReader reader = new LineReader(file);
            String line = reader.nextLine();
            while (line != null) {
                String[] kvs = line.split("\t");
                int p = kvs[0].indexOf(':');
                String key = kvs[0].substring(0, p);
                String value = kvs[0].substring(p + 1);
                System.out.println(value + ", HashCode:" + sun.misc.Hashing.stringHash32(value));

                goodRawRecord.offer(line);
                line = reader.nextLine();
                //count++;
            }
            reader.close();
        }
    }

    public static void main(String[] args) {
        long beginTime = System.currentTimeMillis();
        System.out.print("process good files. begin: " + beginTime );
        Collection<String> files = new ArrayList<String>();
        files.add("./good_records.txt");
        readGoodFile(files);
        System.out.print("process good files. cost: " + (System.currentTimeMillis()-beginTime));

    }
}
