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
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event", "sliding-result"})
@ActiveProfiles("test")
public class MyEventSlidingWindowTest {
    private StreamsBuilder streamsBuilder;
    private Serde<String> stringSerde = new Serdes.StringSerde();
    private TopologyTestDriver topologyTestDriver;
    private Serde<MyEvent> myEventSerde = new MyEventSerde();
    private Serde<MyEventAgg> myEventAggSerde = new MyEventAggSerde();
    private TestInputTopic<String, MyEvent> myEventInput;
    private TestOutputTopic<String, MyEventAgg> slidingOutput;
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
        SlidingWindow slidingWindow = new SlidingWindow(30L, 1L, 0L);
        slidingWindow.processMyEvent(this.streamsBuilder);
        final Topology topology = this.streamsBuilder.build();
        this.topologyTestDriver = new TopologyTestDriver(topology);
        this.myEventInput = this.topologyTestDriver.createInputTopic("my-event", this.stringSerde.serializer(), this.myEventSerde.serializer());
        this.slidingOutput = this.topologyTestDriver.createOutputTopic("sliding-result", this.stringSerde.deserializer(), this.myEventAggSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        if (this.topologyTestDriver != null) {
            this.topologyTestDriver.close();
        }
    }

    @Test
    public void singleKey_eachWindow_maxThreeEvents() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 31L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(2L, "b"), 40L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 55L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(4L, "d"), 100L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 110L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(6L, "z"), 150L);

        List<KeyValue<String, MyEventAgg>> list = this.slidingOutput.readKeyValuesToList();

        assertThat(list, hasSize(8));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(1L));
        assertThat(list.get(0).value.getStr(), is("a"));

        assertThat(list.get(1).value.getFirstSeq(), is(1L));
        assertThat(list.get(1).value.getLastSeq(), is(2L));
        assertThat(list.get(1).value.getStr(), is("ab"));

        assertThat(list.get(2).value.getFirstSeq(), is(1L));
        assertThat(list.get(2).value.getLastSeq(), is(3L));
        assertThat(list.get(2).value.getStr(), is("abc"));

        assertThat(list.get(3).value.getFirstSeq(), is(2L));
        assertThat(list.get(3).value.getLastSeq(), is(3L));
        assertThat(list.get(3).value.getStr(), is("bc"));

        assertThat(list.get(4).value.getFirstSeq(), is(3L));
        assertThat(list.get(4).value.getLastSeq(), is(3L));
        assertThat(list.get(4).value.getStr(), is("c"));

        assertThat(list.get(5).value.getFirstSeq(), is(4L));
        assertThat(list.get(5).value.getLastSeq(), is(4L));
        assertThat(list.get(5).value.getStr(), is("d"));

        assertThat(list.get(6).value.getFirstSeq(), is(4L));
        assertThat(list.get(6).value.getLastSeq(), is(5L));
        assertThat(list.get(6).value.getStr(), is("de"));

        assertThat(list.get(7).value.getFirstSeq(), is(5L));
        assertThat(list.get(7).value.getLastSeq(), is(5L));
        assertThat(list.get(7).value.getStr(), is("e"));
    }

    @Test
    public void multipleKey_eachWindow_maxTwoEvents() {
        this.myEventInput.pipeInput("key1", Util.createMyEvent(1L, "a"), 15L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(2L, "b"), 25L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(3L, "c"), 35L);
        this.myEventInput.pipeInput("key2", Util.createMyEvent(4L, "d"), 47L);
        this.myEventInput.pipeInput("key1", Util.createMyEvent(5L, "e"), 55L);

        this.myEventInput.pipeInput("key1", Util.createMyEvent(99999L, "z"), 9999L);

        List<KeyValue<String, MyEventAgg>> list = this.slidingOutput.readKeyValuesToList();

        assertThat(list, hasSize(8));

        assertThat(list.get(0).value.getFirstSeq(), is(1L));
        assertThat(list.get(0).value.getLastSeq(), is(1L));
        assertThat(list.get(0).value.getStr(), is("a"));

        assertThat(list.get(1).value.getFirstSeq(), is(2L));
        assertThat(list.get(1).value.getLastSeq(), is(2L));
        assertThat(list.get(1).value.getStr(), is("b"));

        assertThat(list.get(2).value.getFirstSeq(), is(1L));
        assertThat(list.get(2).value.getLastSeq(), is(3L));
        assertThat(list.get(2).value.getStr(), is("ac"));

        assertThat(list.get(3).value.getFirstSeq(), is(3L));
        assertThat(list.get(3).value.getLastSeq(), is(3L));
        assertThat(list.get(3).value.getStr(), is("c"));

        assertThat(list.get(4).value.getFirstSeq(), is(2L));
        assertThat(list.get(4).value.getLastSeq(), is(4L));
        assertThat(list.get(4).value.getStr(), is("bd"));

        assertThat(list.get(5).value.getFirstSeq(), is(3L));
        assertThat(list.get(5).value.getLastSeq(), is(5L));
        assertThat(list.get(5).value.getStr(), is("ce"));

        assertThat(list.get(6).value.getFirstSeq(), is(4L));
        assertThat(list.get(6).value.getLastSeq(), is(4L));
        assertThat(list.get(6).value.getStr(), is("d"));

        assertThat(list.get(7).value.getFirstSeq(), is(5L));
        assertThat(list.get(7).value.getLastSeq(), is(5L));
        assertThat(list.get(7).value.getStr(), is("e"));
    }

//    @Test
//    public void demonstrateFullWindowCapture() {
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "a", LinkStatusEnum.DOWN), 31L);
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "b", LinkStatusEnum.DOWN), 40L);
//
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "c", LinkStatusEnum.DOWN), 65L);
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "d", LinkStatusEnum.DOWN), 75L);
//
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "e", LinkStatusEnum.DOWN), 110L);
//        this.linkStatusInput.pipeInput("testLink 1", Util.createLink("testLink1", "z", LinkStatusEnum.DOWN), 300L);
//
//        List<KeyValue<String, LinkMonitor>> list = this.slidingOutput.readKeyValuesToList();
//
//        assert list.size() == 8;
//
//        assert list.get(0).value.getCodes().equals("a");
//        assert list.get(1).value.getCodes().equals("ab");
//        assert list.get(2).value.getCodes().equals("b");
//        assert list.get(3).value.getCodes().equals("bc");
//        assert list.get(4).value.getCodes().equals("c");
//        assert list.get(5).value.getCodes().equals("cd");
//        assert list.get(6).value.getCodes().equals("d");
//        assert list.get(7).value.getCodes().equals("e");
//    }
}
