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
public class TumblingWindow {
    private final Long windowDuration;
    private final Long windowGrace;

    public TumblingWindow(@Value("${tumbling.processor.window.duration:10000}") Long windowDuration,
                          @Value("${tumbling.processor.window.grace:0}") Long windowGrace) {
        this.windowDuration = windowDuration;
        this.windowGrace = windowGrace;
    }

    @Autowired
    public void processMyEvent(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<MyEvent> myEventSerde = new MyEventSerde();
        Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();

        streamsBuilder.stream("my-event", Consumed.with(stringSerde, myEventSerde))
                .peek((k, v) -> log.info("tumbling input {} : {}", k, v))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(this.windowDuration), Duration.ofMillis(this.windowGrace)))
                .aggregate(() -> new MyEventAgg(),
                        ProcessorUtil::aggregateMyEvent,
                        Materialized.<String, MyEventAgg, WindowStore<Bytes, byte[]>>as("tumbling-window-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(myEventAggSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .peek((k, v) -> log.info("tumbling output {} : {}", k, v))
                .to("tumbling-result", Produced.with(stringSerde, myEventAggSerde));

    }

}
