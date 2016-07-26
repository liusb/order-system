package com.alibaba.middleware.race.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class LineReader {
    private BufferedReader reader;

    public LineReader(String file) {
        try {
            reader = new BufferedReader(new FileReader(file), 16*(1024));
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String nextLine() {
        try {
            return reader.readLine();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String readLine() throws IOException {
        return reader.readLine();
    }

    public void close() {
        try {
            reader.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}