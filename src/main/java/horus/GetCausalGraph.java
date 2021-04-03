package horus;

import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import horus.causality.CausalNode;
import horus.causality.LogicalTimeAssignmentIterator;
import horus.causality.OnlyLogsLogicalTimeAssignmentIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class GetCausalGraph {
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This declares the first of two procedures in this class - a
     * procedure that performs queries in a legacy index.
     * <p>
     * It returns a Stream of Records, where records are
     * specified per procedure. This particular procedure returns
     * a stream of {@link VirtualGraph} records.
     * <p>
     * The arguments to this procedure are annotated with the
     * {@link Name} annotation and define the position, name
     * and type of arguments required to invoke this procedure.
     * There is a limited set of types you can use for arguments,
     * these are as follows:
     *
     * <ul>
     * <li>{@link String}</li>
     * <li>{@link Long} or {@code long}</li>
     * <li>{@link Double} or {@code double}</li>
     * <li>{@link Number}</li>
     * <li>{@link Boolean} or {@code boolean}</li>
     * <li>{@link Map} with key {@link String} and value {@link Object}</li>
     * <li>{@link List} of elements of any valid argument type, including {@link List}</li>
     * <li>{@link Object}, meaning any of the valid argument types</li>
     * </ul>
     *
     * @return the nodes found by the query
     */
    @Procedure(value = "horus.getCausalGraph", mode = Mode.READ)
    @Description("Get vector clocks of the given nodes.")
    public Stream<NodeHit> getCausalGraph(@Name("start") Node start,
                                          @Name("end") Node end,
                                          @Name(value = "onlyLogs", defaultValue = "false") Boolean onlyLogs,
                                          @Name(value = "filterHosts", defaultValue = "[]") List<String> filterHosts) {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("fromNodeId", start.getProperty("eventId"));
        parameters.put("toNodeId", end.getProperty("eventId"));

        Result pathsResult = db.execute("MATCH (from:EVENT {eventId: $fromNodeId})\n" +
                "MATCH (to:EVENT {eventId: $toNodeId})\n" +
                "CALL horus.getCausalNodes(from, to) YIELD node\n" +
                "WITH collect(node) as nodes\n" +
                "OPTIONAL MATCH p = (n)-[r]->(m)\n" +
                "WHERE n IN nodes AND m IN nodes\n" +
                "RETURN p", parameters);

        List<Path> paths = pathsResult.stream().map(result -> (Path) result.get("p")).collect(Collectors.toList());
        List<String> virtualNodesProperties = new ArrayList<String>() {{
            add("eventId");
            add("threadId");
            add("host");
            add("message");
        }};

        HashMap<Long, Node> virtualNodes = new HashMap<>();
        HashMap<Long, CausalNode> causalNodes = new HashMap<>();
        List<Pair<CausalNode, CausalNode>> causalPairs= new ArrayList<>();
        for (Path path : paths) {
            Node startNode = path.startNode();
            Node endNode = path.endNode();

            virtualNodes.computeIfAbsent(startNode.getId(), id -> new VirtualNode(startNode, virtualNodesProperties));
            virtualNodes.computeIfAbsent(endNode.getId(), id -> new VirtualNode(endNode, virtualNodesProperties));

            causalNodes.computeIfAbsent(startNode.getId(), id -> new CausalNode(virtualNodes.get(startNode.getId())));
            causalNodes.computeIfAbsent(endNode.getId(), id -> new CausalNode(virtualNodes.get(endNode.getId())));

            causalPairs.add(new ImmutablePair<>(
                    causalNodes.get(startNode.getId()),
                    causalNodes.get(endNode.getId())
            ));
        }

        Graph<CausalNode, DefaultEdge> causalGraph = new SimpleDirectedGraph<>(DefaultEdge.class);

        for(CausalNode causalNode : causalNodes.values()) {
            causalGraph.addVertex(causalNode);
        }

        for(Pair<CausalNode, CausalNode> causalPair : causalPairs) {
            causalGraph.addEdge(
                    causalPair.getLeft(),
                    causalPair.getRight()
            );
        }


        Stream<CausalNode> nodeStream;
        if (onlyLogs) {
            (new OnlyLogsLogicalTimeAssignmentIterator(causalGraph, causalNodes.get(start.getId()), filterHosts)).forEachRemaining(p -> {});
            nodeStream = causalNodes.values().stream().filter(node -> {
                if (! node.getNode().hasLabel(Label.label("LOG")))
                    return false;

                if (filterHosts.size() > 0 && ! filterHosts.contains((String) node.getNode().getProperty("host", null)))
                    return false;

                return true;
            });
        } else {
            (new LogicalTimeAssignmentIterator(causalGraph, causalNodes.get(start.getId()))).forEachRemaining(p -> {});
            nodeStream = causalNodes.values().stream();
        }

        return nodeStream.map(node -> new NodeHit(node.getNode()));
    }


    /**
     * This is the output record for our search procedure. All procedures
     * that return results return them as a Stream of Records, where the
     * records are defined like this one - customized to fit what the procedure
     * is returning.
     * <p>
     * These classes can only have public non-final fields, and the fields must
     * be one of the following types:
     *
     * <ul>
     * <li>{@link String}</li>
     * <li>{@link Long} or {@code long}</li>
     * <li>{@link Double} or {@code double}</li>
     * <li>{@link Number}</li>
     * <li>{@link Boolean} or {@code boolean}</li>
     * <li>{@link Node}</li>
     * <li>{@link org.neo4j.graphdb.Relationship}</li>
     * <li>{@link org.neo4j.graphdb.Path}</li>
     * <li>{@link Map} with key {@link String} and value {@link Object}</li>
     * <li>{@link List} of elements of any valid field type, including {@link List}</li>
     * <li>{@link Object}, meaning any of the valid field types</li>
     * </ul>
     */
    public static class NodeHit {
        public Node node;

        public NodeHit(Node node) {
            this.node = node;
        }
    }
}
