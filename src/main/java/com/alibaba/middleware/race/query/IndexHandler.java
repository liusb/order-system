package com.alibaba.middleware.race.query;

import java.nio.channels.CompletionHandler;
import java.util.HashMap;

public class IndexHandler implements CompletionHandler<Integer, IndexAttachment> {

    @Override
    public void completed(Integer result, IndexAttachment attachment) {
        HashMap<String, String> record = attachment.record;
        byte[] buffer = attachment.buffer;
        int begin = 0;
        String key = "";
        boolean readAllRow = false;
        for (int i = 0; i < result; i++) {
            if (buffer[i] == '\n') {
                record.put(key, new String(buffer, begin, i - begin));
                readAllRow = true;
                break;
            }
            if (buffer[i] == ':') {
                key = new String(buffer, begin, i - begin);
                begin = i + 1;
            } else if (buffer[i] == '\t') {
                record.put(key, new String(buffer, begin, i - begin));
                begin = i + 1;
            }
        }
        attachment.buffer = null;
        attachment.latch.countDown();
        if (!readAllRow) {
            throw new RuntimeException("ERROR: 没有都到完整的一行数据");
        }
    }

    @Override
    public void failed(Throwable exc, IndexAttachment attachment) {
        System.out.println("ERROR: Find Record failed with exception:");
        exc.printStackTrace();
    }
}
