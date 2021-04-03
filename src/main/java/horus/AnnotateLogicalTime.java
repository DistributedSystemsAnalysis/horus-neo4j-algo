package horus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class AnnotateLogicalTime {
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
     * a stream of {@link PathHit} records.
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
    @Procedure(value = "horus.annotateLogicalTime", mode = Mode.WRITE)
    @Description("Annotate logical time, using Lamport and Vector Clocks, to nodes.")
    public void annotateLogicalTime() {
        // 1. get start nodes
        Result startNodes = db.execute("MATCH (n)\n" +
                "WHERE NOT ()-->(n)" +
                "return n");

        List<Node> nodes = startNodes.stream().map(result -> (Node) result.get("n")).collect(Collectors.toList());

        if (this.log.isDebugEnabled())
            this.log.debug("Starting with nodes: " + nodes.stream().map(Node::getId).collect(Collectors.toList()));

        // 3. get all nodes and relationships from nodes.
        db.traversalDescription()
                .breadthFirst()
                .relationships(Rels.HAPPENS_BEFORE, Direction.OUTGOING)
                .relationships(Rels.happens_before, Direction.OUTGOING)
                .evaluator(new PathEvaluator())
                .uniqueness(Uniqueness.NONE)
                .traverse(nodes)
                .stream()
                .forEach(p -> {
                });
    }

    public class PathEvaluator implements Evaluator {

        private final HashMap<Long, MutableInt> parentRelatedCalculations;

        public PathEvaluator() {
            this.parentRelatedCalculations = new HashMap<>();
        }

        @Override
        public Evaluation evaluate(Path path) {
            Node currentNode = path.endNode();
            String currentNodeThreadId = (String) currentNode.getProperty("threadId");
            int parents = currentNode.getDegree(Direction.INCOMING);

            // If it's a node without parents, then it is the beginning of the timeline.
            if (parents == 0) {
                VectorClock vc = new VectorClock(currentNodeThreadId);
                vc.increment();

                setVectorClockTimestamp(currentNode, vc);
                setLamportClockTimestamp(currentNode, 1L);

                return Evaluation.EXCLUDE_AND_CONTINUE;
            }

            this.parentRelatedCalculations.putIfAbsent(currentNode.getId(), new MutableInt(0));

            // Otherwise, we will check if all parents already have its logical time assigned.
            // If not, we will stop this path and delay the assignment to another path evaluation.
            int currentCalculatedParents = this.parentRelatedCalculations.get(currentNode.getId()).incrementAndGet();
            VectorClock parentVectorTime = getParentVectorTime(path);
            Long parentLamportTime = getParentLamportTime(path);

            boolean continuePath = true;
            VectorClock vc = currentNode.hasProperty("vectorLogicalTime") ?
                    getVectorClockTimestamp(currentNode) :
                    new VectorClock(currentNodeThreadId);

            long lc = currentNode.hasProperty("lamportLogicalTime") ?
                    (Long) currentNode.getProperty("lamportLogicalTime") :
                    1L;

            if (currentCalculatedParents == parents) {
                if (log.isDebugEnabled())
                    log.debug("Parents have assigned logical time. Assigning logical time to " + currentNode.getProperty("eventId"));

                this.parentRelatedCalculations.remove(currentNode.getId());
                vc.merge(parentVectorTime);
                lc = Math.max(lc, parentLamportTime) + 1;
            } else {
                if (log.isDebugEnabled())
                    log.debug("Waiting for parents to have assigned time " + currentNode.getProperty("eventId"));

                vc.mergeWithoutIncrement(parentVectorTime);
                lc = Math.max(lc, parentLamportTime);
                continuePath = false;
            }

            setVectorClockTimestamp(currentNode, vc);
            setLamportClockTimestamp(currentNode, lc);

            return continuePath ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_PRUNE;
        }

        private void setVectorClockTimestamp(Node currentNode, VectorClock vc) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Assigning VC " + vc + " to node " + currentNode.getProperty("eventId"));

                currentNode.setProperty("vectorLogicalTime", (new ObjectMapper()).writeValueAsString(vc.toMap()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Could not set vectorLogicalTime property: " + e.getMessage());
            }
        }

        private void setLamportClockTimestamp(Node currentNode, Long lc) {
            if (log.isDebugEnabled())
                log.debug("Assigning lamport time " + lc + " to node " + currentNode.getProperty("eventId"));

            currentNode.setProperty("lamportLogicalTime", lc);
        }

        private VectorClock getParentVectorTime(Path path) {
            Node parentNode = path.lastRelationship().getStartNode();

            if (parentNode.hasProperty("vectorLogicalTime")) {
                return getVectorClockTimestamp(parentNode);
            }

            return null;
        }

        private Long getParentLamportTime(Path path) {
            Node parentNode = path.lastRelationship().getStartNode();

            if (parentNode.hasProperty("lamportLogicalTime")) {
                return (Long) parentNode.getProperty("lamportLogicalTime");
            }

            return null;
        }

        private VectorClock getVectorClockTimestamp(Node node) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode time = null;
            try {
                time = objectMapper.readTree((String) node.getProperty("vectorLogicalTime"));
            } catch (IOException e) {
                throw new RuntimeException("Could not get vectorLogicalTime property.");
            }

            Map<String, Integer> timeMap = new HashMap<>();
            time.fields().forEachRemaining(entry -> {
                if (!entry.getValue().isInt())
                    throw new RuntimeException("Unexpected values in vectorLogicalTime field.");

                timeMap.put(entry.getKey(), entry.getValue().intValue());
            });

            return new VectorClock((String) node.getProperty("threadId"), timeMap);
        }
    }

    private enum Rels implements RelationshipType {
        HAPPENS_BEFORE, happens_before
    }
}
