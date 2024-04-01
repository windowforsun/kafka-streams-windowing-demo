package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.KafkaConfig;
import com.windowforsun.kafka.streams.windowing.model.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"link.status", "link.sliding"})
@ActiveProfiles("test")
public class SlidingWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<Link> linkSerde = new LinkSerde();
    private Serde<LinkMonitor> linkMonitorSerde = new LinkMonitorSerde();
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Link> linkStatusInput;
    private TestOutputTopic<String, LinkMonitor> slidingOutput;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @BeforeEach
    public void setUp () {
        this.registry.getListenerContainers()
                .stream()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));

        this.streamsBuilder = new StreamsBuilder();
        SlidingWindow slidingWindow = new SlidingWindow(30L, 0L, 0L);
        slidingWindow.process(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();
        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.linkStatusInput = this.topologyTestDriver.createInputTopic("link.status", this.stringSerde.serializer(), this.linkSerde.serializer());
        this.slidingOutput = this.topologyTestDriver.createOutputTopic("link.sliding", this.stringSerde.deserializer(), this.linkMonitorSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void demonstrateFullWindowCapture() {
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "a", LinkStatusEnum.DOWN), 31L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "b", LinkStatusEnum.DOWN), 40L);

        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "c", LinkStatusEnum.DOWN), 65L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "d", LinkStatusEnum.DOWN), 75L);

        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "e", LinkStatusEnum.DOWN), 110L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "z", LinkStatusEnum.DOWN), 300L);

        List<KeyValue<String, LinkMonitor>> list = this.slidingOutput.readKeyValuesToList();

        System.out.println(list.size());
        assert list.size() == 8;

        assert list.get(0).value.getCodes().equals("a");
        assert list.get(1).value.getCodes().equals("ab");
        assert list.get(2).value.getCodes().equals("b");
        assert list.get(3).value.getCodes().equals("bc");
        assert list.get(4).value.getCodes().equals("c");
        assert list.get(5).value.getCodes().equals("cd");
        assert list.get(6).value.getCodes().equals("d");
        assert list.get(7).value.getCodes().equals("e");
    }
}
