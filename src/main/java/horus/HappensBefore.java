package horus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class HappensBefore {
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
    @Procedure(value = "horus.happensBefore", mode = Mode.READ)
    @Description("Check whether the happens before relation holds for the given two events.")
    public Stream<HappensBeforeResult> happensBefore(@Name("from") Node n1,
                                             @Name("to") Node n2) {

        VectorClock n1Clock = getVectorLogicalTime(n1);
        VectorClock n2Clock = getVectorLogicalTime(n2);

        Stream.Builder<Boolean> builder = Stream.builder();
        builder.add(n1Clock.lessThan(n2Clock));

        return builder.build().map(HappensBeforeResult::new);
    }

    private VectorClock getVectorLogicalTime(Node node) {
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

        VectorClock vc = new VectorClock((String) node.getProperty("threadId"), timeMap);

        return vc;
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
    public static class HappensBeforeResult {
        public Boolean result;

        public HappensBeforeResult(Boolean result) {
            this.result = result;
        }
    }
}
