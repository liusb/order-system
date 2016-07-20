package com.alibaba.middleware.race.index;


public class HashIndex {
    private final int bucketSize;
    private final int bitAnd;
    private final int fileCount;

    public HashIndex(int bucketSize, int fileCount) {
        this.bucketSize = bucketSize;
        this.bitAnd = bucketSize-1;
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
        return (hashCode/bucketSize)%fileCount;
    }

    public int getBucketIndex(int hashCode) {
        return hashCode & this.bitAnd;
    }

}
