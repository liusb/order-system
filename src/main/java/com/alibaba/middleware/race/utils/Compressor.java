package com.alibaba.middleware.race.utils;

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

}
