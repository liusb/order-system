package com.alibaba.middleware.race.index;


public class HashIndex {
    private final int bucketBitSize;
    private final int bitAnd;
    private int fileCount;

    public HashIndex(int bucketBitSize, int fileCount) {
        this.bucketBitSize = bucketBitSize;
        this.bitAnd = (1 << bucketBitSize)-1;
        this.fileCount = fileCount;
    }

    public static int getHashCode(Object key) {
        if (key instanceof String) {
            return sun.misc.Hashing.stringHash32((String) key);
        }

        int h = key.hashCode();
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public int getFileIndex(int hashCode) {
        return (hashCode >>> bucketBitSize)%fileCount;
    }

    public int getBucketIndex(int hashCode) {
        return hashCode & this.bitAnd;
    }

}
