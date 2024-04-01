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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event", "hopping-result"})
@ActiveProfiles("test")
public class MyEventHoppingWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private Serde<MyEvent> myEventSerde = new MyEventSerde();
    private Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();
    private TestInputTopic<String, MyEvent> myEventInput;
    private TestOutputTopic<String, MyEventAgg> hoppingResultOutput;
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
        hoppingWindow.processMyEvent(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();

        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.myEventInput = this.topologyTestDriver.createInputTopic("my-event", this.stringSerde.serializer(), this.myEventSerde.serializer());
        this.hoppingResultOutput = this.topologyTestDriver.createOutputTopic("hopping-result", this.stringSerde.deserializer(), this.myEventAggSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void singleKey_eachWindow_twoEvents() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 0L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(2L, "b"), 7L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 10L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(4L, "d"), 15L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "z"), 30L);

        List<KeyValue<String, MyEventAgg>> list = this.hoppingResultOutput.readKeyValuesToList();

        assertThat(list, hasSize(4));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(2L));
        assertThat(list.get(0).value.getStr(), is("ab"));

        assertThat(list.get(1).value.getFirstSeq(), is(2L));
        assertThat(list.get(1).value.getLastSeq(), is(3L));
        assertThat(list.get(1).value.getStr(), is("bc"));

        assertThat(list.get(2).value.getFirstSeq(), is(3L));
        assertThat(list.get(2).value.getLastSeq(), is(4L));
        assertThat(list.get(2).value.getStr(), is("cd"));

        assertThat(list.get(3).value.getFirstSeq(), is(4L));
        assertThat(list.get(3).value.getLastSeq(), is(4L));
        assertThat(list.get(3).value.getStr(), is("d"));
    }

    @Test
    public void singleKey_eachWindow_oneEvents_duplicated() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 7L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(2L, "b"), 23L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(9999L, "z"), 9999L);

        List<KeyValue<String, MyEventAgg>> list = this.hoppingResultOutput.readKeyValuesToList();

        assertThat(list, hasSize(4));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(1L));
        assertThat(list.get(0).value.getStr(), is("a"));

        assertThat(list.get(1).value.getFirstSeq(), is(1L));
        assertThat(list.get(1).value.getLastSeq(), is(1L));
        assertThat(list.get(1).value.getStr(), is("a"));

        assertThat(list.get(2).value.getFirstSeq(), is(2L));
        assertThat(list.get(2).value.getLastSeq(), is(2L));
        assertThat(list.get(2).value.getStr(), is("b"));

        assertThat(list.get(3).value.getFirstSeq(), is(2L));
        assertThat(list.get(3).value.getLastSeq(), is(2L));
        assertThat(list.get(3).value.getStr(), is("b"));
    }

    @Test
    public void multipleKey_eachWindow_oneEvents_duplicated2() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 7L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(2L, "b"), 10L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(3L, "c"), 16L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(4L, "d"), 19L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "z"), 30L);

        List<KeyValue<String, MyEventAgg>> list = this.hoppingResultOutput.readKeyValuesToList();

        assertThat(list, hasSize(7));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(1L));
        assertThat(list.get(0).value.getStr(), is("a"));

        assertThat(list.get(1).value.getFirstSeq(), is(1L));
        assertThat(list.get(1).value.getLastSeq(), is(1L));
        assertThat(list.get(1).value.getStr(), is("a"));

        assertThat(list.get(2).value.getFirstSeq(), is(2L));
        assertThat(list.get(2).value.getLastSeq(), is(2L));
        assertThat(list.get(2).value.getStr(), is("b"));

        assertThat(list.get(3).value.getFirstSeq(), is(4L));
        assertThat(list.get(3).value.getLastSeq(), is(4L));
        assertThat(list.get(3).value.getStr(), is("d"));

        assertThat(list.get(4).value.getFirstSeq(), is(2L));
        assertThat(list.get(4).value.getLastSeq(), is(3L));
        assertThat(list.get(4).value.getStr(), is("bc"));

        assertThat(list.get(5).value.getFirstSeq(), is(4L));
        assertThat(list.get(5).value.getLastSeq(), is(4L));
        assertThat(list.get(5).value.getStr(), is("d"));

        assertThat(list.get(6).value.getFirstSeq(), is(3L));
        assertThat(list.get(6).value.getLastSeq(), is(3L));
        assertThat(list.get(6).value.getStr(), is("c"));

    }

}
