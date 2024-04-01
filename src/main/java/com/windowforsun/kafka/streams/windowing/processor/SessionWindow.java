package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class SessionWindow {
    private final Long inactivityGap;
    private final Long windowGrade;

    public SessionWindow(@Value("${session.processor.window.inactivity-gap:10000}") Long inactivityGap,
                         @Value("${session.processor.window.grace:1000}") Long windowGrade) {
        this.inactivityGap = inactivityGap;
        this.windowGrade = windowGrade;
    }

    @Autowired
    public void processMyEvent(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<MyEvent> myEventSerde = new MyEventSerde();
        Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();

        Merger<String, MyEventAgg> sessionMerger = (aggKey, aggOne, aggTwo) -> MyEventAgg.builder()
                .firstSeq(Long.min(aggOne.getFirstSeq(), aggTwo.getFirstSeq()))
                .lastSeq(Long.max(aggOne.getLastSeq(), aggTwo.getLastSeq()))
                .count(aggOne.getCount() + aggTwo.getCount())
                .str(aggOne.getStr().concat(aggTwo.getStr()))
                .build();

        streamsBuilder.stream("my-event", Consumed.with(stringSerde, myEventSerde))
                .peek((k, v) -> log.info("session input event {} : {}", k, v))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMillis(this.inactivityGap), Duration.ofMillis(this.windowGrade)))
                .aggregate(() -> new MyEventAgg(),
                        ProcessorUtil::aggregateMyEvent,
                        sessionMerger,
                        Materialized.<String, MyEventAgg, SessionStore<Bytes, byte[]>>as("session-window-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(myEventAggSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .peek((k, v) -> log.info("session output {} : {}", k, v))
                .to("session-result", Produced.with(stringSerde, myEventAggSerde));
    }

}
