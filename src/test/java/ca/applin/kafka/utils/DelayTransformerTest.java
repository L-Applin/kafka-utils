package ca.applin.kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class DelayTransformerTest {

    static final String IN_TOPIC_NAME = "input-topic";
    static final String OUT_TOPIC_NAME = "output-topic";
    static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    TopologyTestDriver driver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    public void setup() {
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = setupTopology(sb);
        this.driver = new TopologyTestDriver(topology, kafkaTestClusterProp());
        this.inputTopic = driver.createInputTopic(IN_TOPIC_NAME, STRING_SERIALIZER, STRING_SERIALIZER);
        this.outputTopic = driver.createOutputTopic(OUT_TOPIC_NAME, STRING_DESERIALIZER, STRING_DESERIALIZER);
    }

    // test topology for the embedded kafka instance.
    // simply reading from topic and pushing to output topic with delay
    private Topology setupTopology(StreamsBuilder sb) {
        sb.<String, String>stream(IN_TOPIC_NAME)
            .transform(() -> new SingleRecordDelayTransformer<>(ofSeconds(5)))
            .to(OUT_TOPIC_NAME);
        return sb.build();
    }

    private Properties kafkaTestClusterProp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:42069");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return props;
    }

    @Test
    void testDelay() {
        inputTopic.pipeInput("first");
        inputTopic.pipeInput("second");
        assertTrue(outputTopic.isEmpty());
        driver.advanceWallClockTime(Duration.ofSeconds(4));
        inputTopic.pipeInput("third");
        assertTrue(outputTopic.isEmpty());
        driver.advanceWallClockTime(Duration.ofSeconds(1));
        assertFalse(outputTopic.isEmpty());
        assertEquals("first", outputTopic.readValue());
        assertEquals("second", outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
        driver.advanceWallClockTime(Duration.ofSeconds(4));
        assertFalse(outputTopic.isEmpty());
        assertEquals("third", outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
    }
}