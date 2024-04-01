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
import java.util.stream.DoubleStream;

@Slf4j
@Component
public class SlidingWindow {
    private final Long windowDuration;
    private final Long windowGrace;
    private final Long linkThreshold;

    public SlidingWindow(@Value("${sliding.processor.window.duration:10000}") Long windowDuration,
                         @Value("${sliding.processor.window.grace:1000}") Long windowGrace,
                         @Value("${sliding.processor.link.threshold:25}") Long linkThreshold) {
        this.windowDuration = windowDuration;
        this.windowGrace = windowGrace;
        this.linkThreshold = linkThreshold;
    }

    @Autowired
    public void processMyEvent(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<MyEvent> myEventSerde = new MyEventSerde();
        Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();

        streamsBuilder.stream("my-event", Consumed.with(stringSerde, myEventSerde))
                .peek((k, v) -> log.info("sliding input {} : {}", k, v))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(this.windowDuration), Duration.ofMillis(this.windowGrace)))
//                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(this.windowDuration)))
//                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(windowDuration)).advanceBy(Duration.ofMillis(1)))
                .aggregate(() -> new MyEventAgg(),
                        ProcessorUtil::aggregateMyEvent,
                        Materialized.<String, MyEventAgg, WindowStore<Bytes, byte[]>>as("sliding-window-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(myEventAggSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .peek((k, v) -> log.info("sliding output {} : {}", k, v))
                .to("sliding-result", Produced.with(stringSerde, myEventAggSerde));
    }

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        Serde<Link> linkSerde = new LinkSerde();
        Serde<LinkMonitor> linkMonitorSerde = new LinkMonitorSerde();
        Serde<String> stringSerde = new Serdes.StringSerde();

        streamsBuilder.stream("link.status", Consumed.with(stringSerde, linkSerde))
                .peek((k, v) -> log.info("sliding peek link.status event {} : {}", k, v))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(this.windowDuration), Duration.ofMillis(this.windowGrace)))
                .aggregate(() -> new LinkMonitor(),
                        this::aggregate,
                        Materialized.<String, LinkMonitor, WindowStore<Bytes, byte[]>>as("sliding-window-link-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(linkMonitorSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((k, v) -> KeyValue.pair(k.key(), v))
                .filter((k, linkMonitor) -> linkMonitor.getDownCount() >= this.linkThreshold)
                .peek((k, v) -> log.info("sliding peek {} : {}", k, v))
                .to("link.sliding", Produced.with(stringSerde, linkMonitorSerde));
    }

    private LinkMonitor aggregate(String key, Link link, LinkMonitor linkMonitor) {
        System.out.println("test !! " + link);
        Long downCount = linkMonitor.getDownCount();
        String codes = linkMonitor.getCodes();

        if (codes == null) {
            codes = "";
        }
        codes = codes.concat(link.getCode());

        if (link.getStatus() == LinkStatusEnum.DOWN) {
            downCount++;
        } else {
            downCount = 0L;
        }

        LinkMonitor newLinkMonitor = LinkMonitor.builder()
                .name(link.getName())
                .ip(link.getIp())
                .downCount(downCount)
                .status(link.getStatus())
                .codes(codes)
                .build();

        log.info("Sliding Aggregated link monitor: {}", newLinkMonitor);
        return newLinkMonitor;
    }
}
