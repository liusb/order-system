package com.alibaba.middleware.race.store;


/**
 * Page中的数据，byte数组data作为缓冲区，pos记录当前的操作位置
 */
public class Data {

    // 存储数据本身
    private byte[] data;
    // 当前读写位置
    private int pos;

    public Data(byte[] data) {
        this.data = data;
        pos = 0;
    }

    public Data(byte[] data, int pos) {
        this.data = data;
        this.pos = pos;
    }

    public byte[] getBytes() {
        return data;
    }

    public int getLength() {
        return data.length;
    }

    public int getEmptySize() {
        return data.length-pos;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    // 跳过len个byte长度
    public void skip(int len) {
        pos = pos + len;
    }

    public void reset() {
        pos = 0;
    }

    public void writeByte(byte x) {
        data[pos++] = x;
    }

    public byte readByte() {
        return data[pos++];
    }

    public void setInt(int pos, int x) {
        byte[] buff = data;
        buff[pos] = (byte) (x >> 24);
        buff[pos + 1] = (byte) (x >> 16);
        buff[pos + 2] = (byte) (x >> 8);
        buff[pos + 3] = (byte) x;
    }

    public void writeInt(int x) {
        setInt(pos, x);
        pos += 4;
    }

    public int readInt() {
        byte[] buff = data;
        int x = (buff[pos] << 24) +
                ((buff[pos+1] & 0xff) << 16) +
                ((buff[pos+2] & 0xff) << 8) +
                (buff[pos+3] & 0xff);
        pos += 4;
        return x;
    }

    public void writeVarInt(int x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) (0x80 | (x & 0x7f));
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    public int readVarInt() {
        int b = data[pos];
        if (b >= 0) {
            pos++;
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(b);
    }

    private int readVarIntRest(int b) {
        int x = b & 0x7f;
        b = data[pos + 1];
        if (b >= 0) {
            pos += 2;
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = data[pos + 2];
        if (b >= 0) {
            pos += 3;
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = data[pos + 3];
        if (b >= 0) {
            pos += 4;
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (data[pos + 4] << 28);
        pos += 5;
        return x;
    }

    public void writeLong(long x) {
        writeInt((int) (x >>> 32));
        writeInt((int) x);
    }

    public long readLong() {
        return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    }

    public void writeVarLong(long x) {
        while ((x & ~0x7f) != 0) {
            data[pos++] = (byte) ((x & 0x7f) | 0x80);
            x >>>= 7;
        }
        data[pos++] = (byte) x;
    }

    public long readVarLong() {
        long x = data[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = data[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    public void writeDouble(double x) {
        writeLong(Double.doubleToLongBits(x));
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public void writeString(String s) {
        int len = s.length();
        writeInt(len);
        writeString(s, len);
    }

    public String readString() {
        int len = readInt();
        return readString(len);
    }

    public void writeString(String s, int len) {
        int p = pos;
        byte[] buff = data;
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff[p++] = (byte) c;
            } else if (c >= 0x800) {
                buff[p++] = (byte) (0xe0 | (c >> 12));
                buff[p++] = (byte) (((c >> 6) & 0x3f));
                buff[p++] = (byte) (c & 0x3f);
            } else {
                buff[p++] = (byte) (0xc0 | (c >> 6));
                buff[p++] = (byte) (c & 0x3f);
            }
        }
        pos = p;
    }

    public String readString(int len) {
        byte[] buff = data;
        int p = pos;
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff[p++] & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12) +
                        ((buff[p++] & 0x3f) << 6) +
                        (buff[p++] & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) +
                        (buff[p++] & 0x3f));
            }
        }
        pos = p;
        return new String(chars);
    }

    public void copyFrom(Data src, int srcPos, int len) {
        System.arraycopy(src.data, srcPos, this.data, this.pos, len);
        this.pos += len;
    }
}
