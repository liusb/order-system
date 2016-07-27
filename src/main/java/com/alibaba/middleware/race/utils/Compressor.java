package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.worker.LineReader;

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Compressor {
    private int level = Deflater.DEFAULT_COMPRESSION;
    private int strategy = Deflater.DEFAULT_STRATEGY;

    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        Deflater deflater = new Deflater(level);
        deflater.setStrategy(strategy);
        deflater.setInput(in, 0, inLen);
        deflater.finish();
        int compressed = deflater.deflate(out, outPos, out.length - outPos);
        if (compressed == 0) {
            // the compressed length is 0, meaning compression didn't work
            // (sounds like a JDK bug)
            // try again, using the default strategy and compression level
            strategy = Deflater.DEFAULT_STRATEGY;
            level = Deflater.DEFAULT_COMPRESSION;
            return compress(in, inLen, out, outPos);
        }
        deflater.end();
        return outPos + compressed;
    }

    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos,
                       int outLen) {
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, inPos, inLen);
        decompresser.finished();
        try {
            int len = decompresser.inflate(out, outPos, outLen);
            if (len != outLen) {
                throw new DataFormatException(len + " " + outLen);
            }
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
        decompresser.end();
    }

    public static void main(String[] args) {
        LineReader lineReader = new LineReader("./prerun_data/disk1/order.0.3");
        String line;
        byte[] strByte;
        long begin = System.currentTimeMillis();
        long before = 0;
        long after = 0;
        byte[] out = new byte[2048];
        out[0] = 1;
        out[1] = 2;
        System.arraycopy(out, 0, out, 2, 2);
        System.out.println(out[3]);
        Compressor compressor = new Compressor();
        while (true) {
            line = lineReader.nextLine();
            if (line == null) {
                break;
            }
            strByte = line.getBytes();
            before += strByte.length;
            after += compressor.compress(strByte, strByte.length, out, 0);
        }
        System.out.println(before + ", " + after + ", " + (System.currentTimeMillis()-begin));

        lineReader.close();
    }

}
