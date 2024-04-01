package com.windowforsun.kafka.streams.windowing.model;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class MyEventAggSerde extends Serdes.WrapperSerde<MyEventAgg> {
    public MyEventAggSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(MyEventAgg.class));
    }
}
