package com.windowforsun.kafka.streams.windowing.model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class LinkSummarySerde extends Serdes.WrapperSerde<LinkSummary> {
    public LinkSummarySerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(LinkSummary.class));
    }
}
