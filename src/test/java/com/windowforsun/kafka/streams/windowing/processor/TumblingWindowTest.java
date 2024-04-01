package com.windowforsun.kafka.streams.windowing.processor;


import com.windowforsun.kafka.streams.windowing.KafkaConfig;
import com.windowforsun.kafka.streams.windowing.model.Link;
import com.windowforsun.kafka.streams.windowing.model.LinkSerde;
import com.windowforsun.kafka.streams.windowing.model.LinkSummary;
import com.windowforsun.kafka.streams.windowing.model.LinkSummarySerde;
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
@EmbeddedKafka(controlledShutdown = true, topics = {"link.status", "link.tumbling"})
@ActiveProfiles("test")
public class TumblingWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<Link> linkSerde = new LinkSerde();
    private Serde<LinkSummary> linkSummarySerde = new LinkSummarySerde();
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Link> linkStatusInput;
    private TestOutputTopic<String, LinkSummary> tumblingOutput;
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
        TumblingWindow tumblingWindow = new TumblingWindow(10L, 0L);
        tumblingWindow.process(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();

        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.linkStatusInput = this.topologyTestDriver.createInputTopic("link.status", this.stringSerde.serializer(),
                this.linkSerde.serializer());
        this.tumblingOutput = this.topologyTestDriver.createOutputTopic("link.tumbling", this.stringSerde.deserializer(),
                linkSummarySerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void demonstrateTumblingWindowSingleKey() {
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "a"), 2L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "b"), 6L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "c"),10L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "d"), 16L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "e"), 32L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "f"), 35L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "z"), 40);

        List<KeyValue<String, LinkSummary>> list = this.tumblingOutput.readKeyValuesToList();

        assert list.size() == 3;

        assert list.get(0).value.getCodes().equals("ab");
        assert list.get(1).value.getCodes().equals("cd");
        assert list.get(2).value.getCodes().equals("ef");

        assert list.get(0).value.getUpCount() == 2;
        assert list.get(0).value.getDownCount() == 0;
    }

    @Test
    public void demonstrateTumblingWindowMultipleKey() {
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "a"), 0L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "b"), 6L);
        this.linkStatusInput.pipeInput("Link 2", Util.createLink("Link2", "a"), 4L);
        this.linkStatusInput.pipeInput("Link 2", Util.createLink("Link2", "b"), 6L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "c"), 12L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "d"), 16L);
        this.linkStatusInput.pipeInput("Link 1", Util.createLink("Link1", "e"), 32L);

        List<KeyValue<String, LinkSummary>> list = this.tumblingOutput.readKeyValuesToList();

        assert list.size() == 3;

        assert list.get(0).value.getCodes().equals("ab");
        assert list.get(0).value.getName().equals("Link1");
        assert list.get(1).value.getCodes().equals("ab");
        assert list.get(1).value.getName().equals("Link2");

        assert list.get(0).value.getUpCount() == 2;
        assert list.get(0).value.getDownCount() == 0;
    }
}
