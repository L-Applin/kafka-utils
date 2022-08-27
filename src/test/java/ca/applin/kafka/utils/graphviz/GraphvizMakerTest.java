package ca.applin.kafka.utils.graphviz;

import ca.applin.kafka.utils.SingleRecordDelayTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class GraphvizMakerTest {

    private static final String EXPECTED_GRAPH =
            "digraph { subgraph Sub_topology_0 { KSTREAM_SOURCE_0000000000 " +
            "[label=\"source:in_topic_1\",class=\"source\"]; KSTREAM_SOURCE_0000000000->KSTREAM_MERGE_0000000002; " +
            "KSTREAM_SOURCE_0000000001 [label=\"source:redirect_topic\",class=\"source\"]; " +
            "KSTREAM_SOURCE_0000000001->KSTREAM_MERGE_0000000002; KSTREAM_MERGE_0000000002 " +
            "[class=\"processor\"]; KSTREAM_MERGE_0000000002->proc_1; proc_1 [class=\"processor\"]; " +
            "proc_1->KSTREAM_BRANCH_0000000004; KSTREAM_BRANCH_0000000004 [class=\"processor\"]; " +
            "KSTREAM_BRANCH_0000000004->KSTREAM_BRANCH_00000000042; KSTREAM_BRANCH_0000000004->KSTREAM_BRANCH_0000000004out_1; " +
            "KSTREAM_BRANCH_0000000004->KSTREAM_BRANCH_00000000040; KSTREAM_BRANCH_00000000040 [class=\"processor\"]; " +
            "KSTREAM_BRANCH_00000000040->KSTREAM_SINK_0000000010; KSTREAM_BRANCH_00000000042 [class=\"processor\"]; " +
            "KSTREAM_BRANCH_00000000042->KSTREAM_SINK_0000000008; KSTREAM_BRANCH_0000000004out_1 [class=\"processor\"]; " +
            "KSTREAM_BRANCH_0000000004out_1->KSTREAM_SINK_0000000006; KSTREAM_SINK_0000000006 [label=\"sink:output_branch\",class=\"sink\"]; " +
            "KSTREAM_SINK_0000000008 [label=\"sink:retry_topic\",class=\"sink\"]; KSTREAM_SINK_0000000010 [label=\"sink:dlt\",class=\"sink\"];  } " +
            "subgraph Sub_topology_1 { KSTREAM_SOURCE_0000000011 [label=\"source:retry_topic\",class=\"source\"]; " +
            "KSTREAM_SOURCE_0000000011->delay_5sec; delay_5sec [class=\"processor\"]; delay_5sec->log_value; " +
            "log_value [class=\"processor\"]; log_value->KSTREAM_SINK_0000000013; " +
            "KSTREAM_SINK_0000000013 [label=\"sink:redirect_topic\",class=\"sink\"];  }  }";

    @Test
    void makeSimpleGraph() {
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

        ByteArrayOutputStream baof = new ByteArrayOutputStream();
        GraphvizMaker graphviz = new GraphvizMaker(new PrintStream(baof));
        final Topology topology = sb.build();
        graphviz.createGraph(topology);

        assertEquals(EXPECTED_GRAPH, baof.toString());

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