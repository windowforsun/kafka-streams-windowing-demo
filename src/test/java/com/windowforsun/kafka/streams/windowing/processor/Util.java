package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.Link;
import com.windowforsun.kafka.streams.windowing.model.LinkStatusEnum;
import com.windowforsun.kafka.streams.windowing.model.MyEvent;

public class Util {
    public static MyEvent createMyEvent(Long seq, String str) {
        return MyEvent.builder()
                .seq(seq)
                .str(str)
                .build();
    }

    public static Link createLink(String name, String code) {
        return createLink(name, code, LinkStatusEnum.UP);
    }

    public static Link createLink(String name, String code, LinkStatusEnum status) {
        Link link = Link.builder()
                .name(name)
                .ip("127.0.0.1")
                .code(code)
                .status(status)
                .bias(1.0)
                .build();

        return link;
    }
}
