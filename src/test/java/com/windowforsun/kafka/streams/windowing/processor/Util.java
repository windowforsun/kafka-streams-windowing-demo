package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.MyEvent;

public class Util {
    public static MyEvent createMyEvent(Long seq, String str) {
        return MyEvent.builder()
                .seq(seq)
                .str(str)
                .build();
    }
}
