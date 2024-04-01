package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.KafkaConfig;
import com.windowforsun.kafka.streams.windowing.model.Link;
import com.windowforsun.kafka.streams.windowing.model.LinkMonitor;
import com.windowforsun.kafka.streams.windowing.model.LinkMonitorSerde;
import com.windowforsun.kafka.streams.windowing.model.LinkSerde;
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
@EmbeddedKafka(controlledShutdown = true, topics = {"link.status", "link.session"})
@ActiveProfiles("test")
public class SessionWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<Link> linkSerde = new LinkSerde();
    private Serde<LinkMonitor> linkMonitorSerde = new LinkMonitorSerde();
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, String> stringInput;
    private TestOutputTopic<String, String> sessionInput;
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
        SessionWindow sessionWindow = new SessionWindow(10L, 0L);
        sessionWindow.process(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();
        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.stringInput = this.topologyTestDriver.createInputTopic("link.status", this.stringSerde.serializer(), this.stringSerde.serializer());
        this.sessionInput = this.topologyTestDriver.createOutputTopic("link.session", this.stringSerde.deserializer(), this.stringSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void test() {
        this.stringInput.pipeInput("test1", "a", 11L);
        this.stringInput.pipeInput("test1", "b", 15L);
        this.stringInput.pipeInput("test1", "c", 20L);
        this.stringInput.pipeInput("test1", "d", 25L);

        this.stringInput.pipeInput("test1", "e", 37L);

        this.stringInput.pipeInput("test1", "f", 50L);
        this.stringInput.pipeInput("test1", "g", 59L);

        this.stringInput.pipeInput("test1", "z", 200L);

        List<KeyValue<String, String>> list = this.sessionInput.readKeyValuesToList();

        assert list.size() == 3;

        assert list.get(0).value.equals("abcd");
        assert list.get(1).value.equals("e");
        assert list.get(2).value.equals("fg");
    }
}
