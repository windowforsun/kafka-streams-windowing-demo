package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class HoppingWindow {
    private final Long windowDuration;
    private final Long windowAdvance;

    public HoppingWindow(@Value("${hopping.processor.window.duration:10000}") Long windowDuration,
                         @Value("${hopping.processor.window.advance:5000}") Long windowAdvance) {
        this.windowDuration = windowDuration;
        this.windowAdvance = windowAdvance;
    }

    @Autowired
    public void processMyEvent(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<MyEvent> myEventSerde = new MyEventSerde();
        Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();

        streamsBuilder
                .stream("my-event", Consumed.with(stringSerde, myEventSerde))
                .peek((k, v) -> log.info("hopping input {} : {}", k, v))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(this.windowDuration)).advanceBy(Duration.ofMillis(this.windowAdvance)))
                .aggregate(() -> new MyEventAgg(),
                        ProcessorUtil::aggregateMyEvent,
                        Materialized.<String, MyEventAgg, WindowStore<Bytes, byte[]>>as("hopping-window-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(myEventAggSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .peek((k, v) -> log.info("hopping output {} : {}", k, v))
                .to("hopping-result", Produced.with(stringSerde, myEventAggSerde));
    }

}
