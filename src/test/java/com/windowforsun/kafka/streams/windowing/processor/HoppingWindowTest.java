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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"link.status", "link.hopping", "my-event", "hopping-result"})
@ActiveProfiles("test")
public class HoppingWindowTest {
    @Test
    public void kjijwgwegwegwe() {
        System.out.println("done");
    }

    private StreamsBuilder streamsBuilder;
    private Serde<Link> linkSerde = new LinkSerde();
    private Serde<LinkSummary> linkSummarySerde = new LinkSummarySerde();
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Link> linkStatusInput;
    private TestOutputTopic<String, LinkSummary> hoppingOutput;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @BeforeEach
    public void setUp() {
        this.registry.getListenerContainers()
                .stream()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));

        this.streamsBuilder = new StreamsBuilder();
        HoppingWindow hoppingWindow = new HoppingWindow(10L, 5L);
        hoppingWindow.process(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();

        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.linkStatusInput = this.topologyTestDriver.createInputTopic("link.status", this.stringSerde.serializer(), this.linkSerde.serializer());
        this.hoppingOutput = this.topologyTestDriver.createOutputTopic("link.hopping", this.stringSerde.deserializer(), this.linkSummarySerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void demonstrateFullWindowCapture() {
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink", "a"), 0L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink", "b"), 7L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink", "c"), 10L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink", "d"), 15L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink", "z"), 30L);

        List<KeyValue<String, LinkSummary>> list = this.hoppingOutput.readKeyValuesToList();

        assert list.size() == 4;

        assert list.get(0).value.getCodes().equals("ab");
        assert list.get(1).value.getCodes().equals("bc");
        assert list.get(2).value.getCodes().equals("cd");
        assert list.get(3).value.getCodes().equals("d");

        assert list.get(0).value.getUpCount() == 2L;
        assert list.get(1).value.getUpCount() == 2L;
    }

    @Test
    public void demonstrateFullWindowWithGapCapture() {
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "a"), 7L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "b"), 23L);
        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "z"), 100L);

        List<KeyValue<String, LinkSummary>> list = this.hoppingOutput.readKeyValuesToList();

        assert list.size() == 4;
        assert list.get(0).value.getCodes().equals("a");
        assert list.get(1).value.getCodes().equals("a");
        assert list.get(2).value.getCodes().equals("b");
        assert list.get(2).value.getCodes().equals("b");
    }

}
