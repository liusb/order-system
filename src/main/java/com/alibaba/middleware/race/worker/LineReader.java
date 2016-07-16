package com.alibaba.middleware.race.worker;

import java.io.BufferedReader;
import java.io.FileReader;


public class LineReader {
    private BufferedReader reader;

    public LineReader(String file) {
        try {
            reader = new BufferedReader(new FileReader(file));
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

    public void close() {
        try {
            reader.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}