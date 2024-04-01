package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.*;

public class ProcessorUtil {

    public static MyEventAgg aggregateMyEvent(String key, MyEvent myEvent, MyEventAgg aggregateMyEvent) {
        Long firstSeq = aggregateMyEvent.getFirstSeq();
        Long lastSeq = aggregateMyEvent.getLastSeq();
        Long count = aggregateMyEvent.getCount();
        String str = aggregateMyEvent.getStr();

        if(count == null || count <= 0) {
            firstSeq = myEvent.getSeq();
            str = "";
            count = 0L;
            lastSeq = Long.MIN_VALUE;
        }

        lastSeq = Long.max(lastSeq, myEvent.getSeq());
        str = str.concat(myEvent.getStr());
        count++;

        return MyEventAgg.builder()
                .firstSeq(firstSeq)
                .lastSeq(lastSeq)
                .count(count)
                .str(str)
                .build();
    }
}
