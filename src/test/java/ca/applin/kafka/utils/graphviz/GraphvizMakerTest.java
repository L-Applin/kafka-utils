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
        "digraph { subgraph Sub_topology_0 { \"in_1\" [label=\"source:in_topic_1\",class=\"source\"]; \"in_1\"->\"merge_input\"; " +
                "\"redirect_in\" [label=\"source:redirect_topic\",class=\"source\"]; \"redirect_in\"->\"merge_input\"; " +
                "\"merge_input\" [class=\"processor\"]; \"merge_input\"->\"proc_1\"; \"proc_1\" [class=\"processor\"]; " +
                "\"proc_1\"->\"async_retry_split.\"; \"async_retry_split.\" [class=\"processor\"]; " +
                "\"async_retry_split.\"->\"async_retry_split.dlt\"; \"async_retry_split.\"->\"async_retry_split.out_1\"; " +
                "\"async_retry_split.\"->\"async_retry_split.retry\"; \"async_retry_split.dlt\" [class=\"processor\"]; " +
                "\"async_retry_split.dlt\"->\"branch_dlt\"; \"async_retry_split.out_1\" [class=\"processor\"]; " +
                "\"async_retry_split.out_1\"->\"branch_1\"; \"async_retry_split.retry\" [class=\"processor\"]; " +
                "\"async_retry_split.retry\"->\"branch_retry\"; \"branch_1\" [label=\"sink:output_branch\",class=\"sink\"]; " +
                "\"branch_dlt\" [label=\"sink:dlt\",class=\"sink\"]; \"branch_retry\" [label=\"sink:retry_topic\",class=\"sink\"]; } " +
                "subgraph Sub_topology_1 { \"retry_in\" [label=\"source:retry_topic\",class=\"source\"]; \"retry_in\"->\"delay_5sec\"; " +
                "\"delay_5sec\" [class=\"processor\"]; \"delay_5sec\"->\"log_value\"; \"log_value\" [class=\"processor\"]; " +
                "\"log_value\"->\"redirect_out\"; \"redirect_out\" [label=\"sink:redirect_topic\",class=\"sink\"]; } }";

    @Test
    void makeSimpleGraph() {
        StreamsBuilder sb = new StreamsBuilder();

        KStream<String, String> mainStream = sb.stream("in-topic-1", Consumed.with(String(), String()).withName("in-1"));
        mainStream.merge(sb.stream("redirect-topic", Consumed.with(String(), String()).withName("redirect-in")), Named.as("merge-input"))
                .transformValues(DummyValueTranformer::new, Named.as("proc-1"))
                .split(Named.as("async-retry-split."))
                .branch((k, v) -> k.length() < 10, Branched.<String, String>withConsumer(ks -> ks.to("output-branch", Produced.as("branch-1"))).withName("out-1"))
                .branch((k, v) -> k.length() >= 10, Branched.<String, String>withConsumer(ks -> ks.to("retry-topic", Produced.as("branch-retry"))).withName("retry"))
                .defaultBranch(Branched.<String, String>withConsumer(ks -> ks.to("dlt", Produced.as("branch-dlt"))).withName("dlt"));

        sb.stream("retry-topic", Consumed.with(String(), String()).withName("retry-in"))
                .transform(() -> new SingleRecordDelayTransformer<>(Duration.ofSeconds(5)), Named.as("delay-5sec"))
                .peek((k, v) -> log.info("{}", v), Named.as("log-value"))
                .to("redirect-topic", Produced.as("redirect-out"));

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