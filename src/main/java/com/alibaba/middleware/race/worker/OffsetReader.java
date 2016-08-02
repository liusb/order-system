package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.table.OffsetLine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OffsetReader implements Runnable {
    private ArrayList<LinkedBlockingQueue<OffsetLine>> outs;
    private HashMap<String, Byte> files;

    public OffsetReader(HashMap<String, Byte> files, ArrayList<LinkedBlockingQueue<OffsetLine>> outs) {
        this.outs = outs;
        this.files = files;
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();
        int step = (int) (threadId % 3) + 1;
        int i;
        int outSize = outs.size();
        long lineCount = 0;
        final int B_SIZE = 1024*1024;
        final byte[] bArray = new byte[B_SIZE+1024];
        int maxLineSize = 0;
        try {
            for (Map.Entry<String, Byte> entry : this.files.entrySet()) {
                FileChannel fileChannel = FileChannel.open(Paths.get(entry.getKey()));
                byte fileId = entry.getValue();
                long nextLineOffset = 0;

                int lineBegin = 0;
                int lineLength = 0;
                int bArrayOffset = 0;
                int getSize;
                int bArrayDataSize;
                ByteBuffer warpedBuffer = ByteBuffer.wrap(bArray, bArrayOffset, B_SIZE);
                while ((getSize = fileChannel.read(warpedBuffer)) != -1) {
                    for (bArrayDataSize = bArrayOffset + getSize; bArrayOffset < bArrayDataSize; bArrayOffset++) {
                        if (bArray[bArrayOffset]=='\n') {
                            lineLength = bArrayOffset-lineBegin;
                            OffsetLine orderLine = new OffsetLine(new RecordIndex(fileId, (int)nextLineOffset),
                                    new String(bArray, lineBegin, lineLength), lineLength+1);
                            if (maxLineSize < lineLength) {
                                maxLineSize = lineLength;
                            }
                            nextLineOffset += (lineLength+1);
                            lineBegin = bArrayOffset+1;
                            while (true) {
                                try {
                                    for (i = 1; i < 16; i++) {
                                        if (outs.get((int) (lineCount + i * step) % outSize)
                                                .offer(orderLine, 420, TimeUnit.MICROSECONDS)) {
                                            break;
                                        }
                                    }
                                    if (i == 16) {
                                        outs.get((int) (lineCount + threadId) % outSize).put(orderLine);
                                    }
                                    break;
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            lineCount++;
                        }
                    }
                    bArrayOffset = bArrayDataSize-lineBegin;
                    System.arraycopy(bArray, lineBegin, bArray, 0, bArrayOffset);
                    lineBegin = 0;
                    warpedBuffer = ByteBuffer.wrap(bArray, bArrayOffset, Math.min(B_SIZE, B_SIZE-bArrayDataSize));
                }
                System.out.println("INFO: reade file " + entry.getKey() + " completed. max offset:" + nextLineOffset);
                fileChannel.close();
            }
            System.out.println("INFO: Reader thread completed. lineCount:"
                    + lineCount + " max line size: " + maxLineSize + " Thread id:" + threadId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
