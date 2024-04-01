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

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event", "session-result"})
@ActiveProfiles("test")
public class MyEventSessionWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private Serde<MyEvent> myEventSerde = new MyEventSerde();
    private Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();
    private TestInputTopic<String, MyEvent> myEventInput;
    private TestOutputTopic<String, MyEventAgg> sessionOutput;
//    private TestInputTopic<String, String> stringInput;
//    private TestOutputTopic<String, String> sessionInput;
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
        sessionWindow.processMyEvent(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();
        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.myEventInput = this.topologyTestDriver.createInputTopic("my-event", this.stringSerde.serializer(), this.myEventSerde.serializer());
        this.sessionOutput = this.topologyTestDriver.createOutputTopic("session-result", this.stringSerde.deserializer(), this.myEventAggSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void singleKey() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 11L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(2L, "b"), 15L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 20L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(4L, "d"), 25L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 37L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(6L, "f"), 50L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(7L, "g"), 59L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(9999L, "z"), 9999L);

        List<KeyValue<String, MyEventAgg>> list = this.sessionOutput.readKeyValuesToList();

        assertThat(list, hasSize(3));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(4L));
        assertThat(list.get(0).value.getStr(), is("abcd"));

        assertThat(list.get(1).value.getFirstSeq(), is(5L));
        assertThat(list.get(1).value.getLastSeq(), is(5L));
        assertThat(list.get(1).value.getStr(), is("e"));

        assertThat(list.get(2).value.getFirstSeq(), is(6L));
        assertThat(list.get(2).value.getLastSeq(), is(7L));
        assertThat(list.get(2).value.getStr(), is("fg"));
    }

    @Test
    public void multipleKey() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 11L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(2L, "b"), 15L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 20L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(4L, "d"), 25L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 37L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(6L, "f"), 50L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(7L, "g"), 59L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(9999L, "z"), 9999L);

        List<KeyValue<String, MyEventAgg>> list = this.sessionOutput.readKeyValuesToList();

        assertThat(list, hasSize(5));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(3L));
        assertThat(list.get(0).value.getStr(), is("ac"));

        assertThat(list.get(1).value.getFirstSeq(), is(2L));
        assertThat(list.get(1).value.getLastSeq(), is(4L));
        assertThat(list.get(1).value.getStr(), is("bd"));

        assertThat(list.get(2).value.getFirstSeq(), is(5L));
        assertThat(list.get(2).value.getLastSeq(), is(5L));
        assertThat(list.get(2).value.getStr(), is("e"));

        assertThat(list.get(3).value.getFirstSeq(), is(6L));
        assertThat(list.get(3).value.getLastSeq(), is(6L));
        assertThat(list.get(3).value.getStr(), is("f"));

        assertThat(list.get(4).value.getFirstSeq(), is(7L));
        assertThat(list.get(4).value.getLastSeq(), is(7L));
        assertThat(list.get(4).value.getStr(), is("g"));
    }
}
