package com.windowforsun.kafka.streams.windowing.model;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class DemoEventSerde extends Serdes.WrapperSerde<DemoEvent> {
    public DemoEventSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(DemoEvent.class));
    }
}
