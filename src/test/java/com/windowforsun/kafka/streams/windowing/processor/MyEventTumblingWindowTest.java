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
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event", "tumbling-result"})
@ActiveProfiles("test")
public class MyEventTumblingWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private Serde<MyEvent> myEventSerde = new MyEventSerde();
    private Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();
    private TestInputTopic<String, MyEvent> myEventInput;
    private TestOutputTopic<String, MyEventAgg> tumblingResultOutput;
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
        tumblingWindow.processMyEvent(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();

        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.myEventInput = this.topologyTestDriver.createInputTopic("my-event",
                this.stringSerde.serializer(),
                this.myEventSerde.serializer());
        this.tumblingResultOutput = this.topologyTestDriver.createOutputTopic("tumbling-result",
                this.stringSerde.deserializer(),
                this.myEventAggSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void singleKey_eachWindow_twoEvents() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 2L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(2L, "b"), 6L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 10L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(4L, "d"), 16L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 32L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(6L, "f"), 36L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(40L, "z"), 40L);

        List<KeyValue<String, MyEventAgg>> list = this.tumblingResultOutput.readKeyValuesToList();

        assertThat(list, hasSize(3));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(2L));
        assertThat(list.get(0).value.getStr(), is("ab"));

        assertThat(list.get(1).value.getFirstSeq(), is(3L));
        assertThat(list.get(1).value.getLastSeq(), is(4L));
        assertThat(list.get(1).value.getStr(), is("cd"));

        assertThat(list.get(2).value.getFirstSeq(), is(5L));
        assertThat(list.get(2).value.getLastSeq(), is(6L));
        assertThat(list.get(2).value.getStr(), is("ef"));
    }

    @Test
    public void multipleKey_eachWindow_twoEvents() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 2L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(2L, "b"), 4L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 6L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(4L, "d"), 8L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 13L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(6L, "g"), 18L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(7L, "f"), 22L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(8L, "h"), 28L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(9L, "z"), 30L);

        List<KeyValue<String, MyEventAgg>> list = this.tumblingResultOutput.readKeyValuesToList();

        assertThat(list, hasSize(4));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(3L));
        assertThat(list.get(0).value.getStr(), is("ac"));

        assertThat(list.get(1).value.getFirstSeq(), is(2L));
        assertThat(list.get(1).value.getLastSeq(), is(4L));
        assertThat(list.get(1).value.getStr(), is("bd"));

        assertThat(list.get(2).value.getFirstSeq(), is(5L));
        assertThat(list.get(2).value.getLastSeq(), is(6L));
        assertThat(list.get(2).value.getStr(), is("eg"));

        assertThat(list.get(3).value.getFirstSeq(), is(7L));
        assertThat(list.get(3).value.getLastSeq(), is(8L));
        assertThat(list.get(3).value.getStr(), is("fh"));
    }
}
