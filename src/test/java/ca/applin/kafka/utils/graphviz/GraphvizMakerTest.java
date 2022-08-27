package ca.applin.kafka.utils.graphviz;

import ca.applin.kafka.utils.SingleRecordDelayTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;

import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
class GraphvizMakerTest {

    @Test
    void makeSimpleGraph() throws IOException {
        StreamsBuilder sb = new StreamsBuilder();

        KStream<String, String> mainStream = sb.stream("in-topic-1", Consumed.with(String(), String()));
        mainStream.merge(sb.stream("redirect-topic", Consumed.with(String(), String())))
                .transformValues(DummyValueTranformer::new, Named.as("proc-1"))
                .split()
                .branch((k, v) -> k.length() < 10, Branched.<String, String>withConsumer(ks -> ks.to("output-branch")).withName("out-1"))
                .branch((k, v) -> k.length() >= 10, Branched.withConsumer(ks -> ks.to("retry-topic")))
                .defaultBranch(Branched.withConsumer(ks -> ks.to("dlt")));

        sb.stream("retry-topic")
                .transform(() -> new SingleRecordDelayTransformer<>(Duration.ofSeconds(5)), Named.as("delay-5sec"))
                .peek((k, v) -> log.info("{}", v), Named.as("log-value"))
                .to("redirect-topic");

        File testFile = new File("src/test/resources/test-topology.dot");

        GraphvizMaker graphviz = new GraphvizMaker(new PrintStream(testFile));
        final Topology topology = sb.build();
        log.info("{}", topology.describe());
        graphviz.createGraph(topology);


    }

    static class DummyValueTranformer implements ValueTransformer<String, String> {
        @Override
        public void init(ProcessorContext context) {}

        @Override
        public String transform(String value) { return value; }

        @Override
        public void close() {}
    }

}