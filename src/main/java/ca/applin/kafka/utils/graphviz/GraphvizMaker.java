package ca.applin.kafka.utils.graphviz;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Takes a kafka streams {@link Topology topology} and creates a Graphviz file representing that topology.
 *
 *
 * @see <a href="https://graphviz.org/">Graphviz</a>
 * @author L-Applin
 */
@RequiredArgsConstructor
@Slf4j
// todo attributes
// todo use struct??
public class GraphvizMaker {

    public static final String DIGRAPH = "digraph";
    public static final String SUBGRAPH = "subgraph";
    public static final String ATTR_LABEL = "label";
    public static final String ATTR_CLASS = "class";
    public static final String PROCESSOR = "processor";

    public static final String EMPTY_GRPAH = DIGRAPH + "{ }";
    public static final String SINK = "sink";
    public static final String SOURCE = "source";

    private record GraphvizAttr(String name, String value) { }

    private final PrintStream ps;

    public void createGraph(Topology topology) {
        if (topology == null) {
            log.warn("Topology is null, cannot create Graphviz file.");
            output(EMPTY_GRPAH);
            return ;
        }
        Set<TopologyDescription.Subtopology> subtopologies = topology.describe().subtopologies();
        output(DIGRAPH + " { ");
        subtopologies.forEach(subtopology -> {
            output(SUBGRAPH + " Sub_topology_" + subtopology.id() + " { ");
            for (TopologyDescription.Node node : subtopology.nodes()) {
                if (node instanceof TopologyDescription.Source source) {
                    makeNode(source);
                }
                if (node instanceof TopologyDescription.Sink sink) {
                    makeNode(sink);
                }
                if (node instanceof TopologyDescription.Processor processor) {
                    makeNode(processor);
                }
                node.successors().forEach(succ -> makeDirectedEdge(node.name(), succ.name()));
            }
            output(" } ");
        });
        output(" }");
        ps.flush();
    }

    private void makeNode(TopologyDescription.Processor processor) {
        output(processor.name());
        makeAttributes(new GraphvizAttr(ATTR_CLASS, PROCESSOR));
        output("; ");
    }

    private void makeNode(TopologyDescription.Sink sink) {
        output(sink.name());
        makeAttributes(
                new GraphvizAttr(ATTR_LABEL, SINK + ":" + sink.topic()),
                new GraphvizAttr(ATTR_CLASS, SINK));
        output("; ");
    }

    private void makeNode(TopologyDescription.Source source) {
        output(source.name());
        String topicStr = source.topicPattern() == null
                ? String.join(",", source.topicSet()) + ""
                : source.topicPattern().pattern();
        makeAttributes(
                new GraphvizAttr(ATTR_LABEL, SOURCE + ":" + topicStr),
                new GraphvizAttr(ATTR_CLASS, SOURCE));
        output("; ");
    }

    private void makeDirectedEdge(String from, String to) {
        output(from);
        ps.print("->");
        output(to + "; ");
    }

    private void makeAttributes(GraphvizAttr... attrs) {
        makeAttributes(Arrays.asList(attrs));
    }

    private void makeAttributes(List<GraphvizAttr> attrs) {
        output(" [");
        for (int i = 0; i < attrs.size(); i++) {
            GraphvizAttr attr = attrs.get(i);
            output(attr.name());
            output("=");
            output("\"");
            output(attr.value());
            output("\"");
            if (i < attrs.size() - 1) {
                output(",");
            }
        }
        output("]");
    }

    private void output(String str) {
        ps.print(str.replace('-', '_'));
    }

}
