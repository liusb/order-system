package com.alibaba.middleware.race.worker;

import com.alibaba.middleware.race.index.RecordIndex;
import com.alibaba.middleware.race.table.OrderLine;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class OrderReader implements Runnable {
    private ArrayList<LinkedBlockingQueue<OrderLine>> outs;
    private HashMap<String, Byte> files;

    public OrderReader(HashMap<String, Byte> files, ArrayList<LinkedBlockingQueue<OrderLine>> outs) {
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
        final int B_SIZE = 8*1024;
        final long MAP_BUFFER_SIZE = 1<<30;
        try {
            for (Map.Entry<String, Byte> entry : this.files.entrySet()) {
                FileChannel fileChannel = FileChannel.open(Paths.get(entry.getKey()));
                long fileSize = fileChannel.size();
                byte fileId = entry.getValue();
                long nextLineOffset = 0;
                int lineBegin = 0;
                byte[] bArray = new byte[B_SIZE];
                int bArrayOffset = 0;
                int mapTime = (int)((fileSize-1)/MAP_BUFFER_SIZE)+1;
                for (int j=0; j < mapTime; j++) {
                    long mapBegin = (MAP_BUFFER_SIZE)*j;
                    long mapSize = Math.min(MAP_BUFFER_SIZE, fileSize - mapBegin);
                    MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapBegin, mapSize);
                    while (buffer.hasRemaining()) {
                        int getSize = Math.min(buffer.remaining(), B_SIZE-bArrayOffset);
                        buffer.get(bArray, bArrayOffset, getSize);
                        int bArrayDataSize = bArrayOffset + getSize;
                        for ( ; bArrayOffset < bArrayDataSize; bArrayOffset++) {
                            if (bArray[bArrayOffset]=='\n') {
                                OrderLine orderLine = new OrderLine(new RecordIndex(fileId, nextLineOffset),
                                        new String(bArray, lineBegin, bArrayOffset-lineBegin));
                                nextLineOffset += (bArrayOffset-lineBegin+1);
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
                        bArrayOffset = bArrayOffset-lineBegin;
                        System.arraycopy(bArray, lineBegin, bArray, 0, bArrayOffset);
                        lineBegin = 0;
                    }
                }
                fileChannel.close();
            }
            System.out.println("INFO: Reader thread completed. lineCount:" + lineCount + " Thread id:" + threadId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
