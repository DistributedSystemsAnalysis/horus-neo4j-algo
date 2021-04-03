package horus;

import apoc.result.VirtualNode;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.impl.core.NodeProxy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class GetCausalGraphTest {
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure(GetCausalGraph.class)
            .withProcedure(GetCausalNodes.class);

    @Test
    public void calculatesCausalNodesBetweenTwoEvents() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryption().toConfig());
             Session session = driver.session()) {

            this.createServerClientExample(session);

            testResult(this.neo4j.getGraphDatabaseService(),
                            "MATCH (start {eventId: 'cloud83.cluster.lsd.di.uminho.pt3'}), (stop {eventId: 'cloud83.cluster.lsd.di.uminho.pt10'}) " +
                            "CALL horus.getCausalGraph(start, stop) YIELD node RETURN node",
                    res -> {
                        String[] causalNodeIds = {
                                "cloud83.cluster.lsd.di.uminho.pt3",
                                "cloud83.cluster.lsd.di.uminho.pt4",
                                "cloud83.cluster.lsd.di.uminho.pt5",
                                "cloud83.cluster.lsd.di.uminho.pt6",
                                "cloud83.cluster.lsd.di.uminho.pt7",
                                "cloud83.cluster.lsd.di.uminho.pt8",
                                "cloud83.cluster.lsd.di.uminho.pt10",
                        };

                        List<String> expectedNodeIds = Arrays.asList(causalNodeIds);
                        List<String> actualNodeIds = res.stream().map(node -> (String) ((VirtualNode) node.get("node")).getProperty("eventId")).collect(Collectors.toList());

                        assertEquals(expectedNodeIds.size(), actualNodeIds.size());

                        expectedNodeIds.sort(String::compareTo);
                        actualNodeIds.sort(String::compareTo);
                        assertArrayEquals(expectedNodeIds.toArray(), actualNodeIds.toArray());
                    });
        }
    }

    private void createServerClientExample(Session session) {
        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("CREATE (:`CREATE`:`UNIQUE IMPORT LABEL` {`childPid`:1911, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt1\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561205963, `lamportLogicalTime`:1, `vectorLogicalTime`:\"{\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":1}\", `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32812});");
                tx.run("CREATE (:`START`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt2\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561205963, `lamportLogicalTime`:2, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":1,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":1}\", `pid`:1900, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32813});");
                tx.run("CREATE (:`CREATE`:`UNIQUE IMPORT LABEL` {`childPid`:1912, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt3\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561239389, `lamportLogicalTime`:2, `vectorLogicalTime`:\"{\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32814});");
                tx.run("CREATE (:`START`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt4\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561239389, `lamportLogicalTime`:3, `vectorLogicalTime`:\"{\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":1,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1900, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147976998, `UNIQUE IMPORT ID`:32815});");
                tx.run("CREATE (:`END`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt10\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562869149, `lamportLogicalTime`:8, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":3,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":4,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1900, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147978004, `UNIQUE IMPORT ID`:32816});");
                tx.run("CREATE (:`JOIN`:`UNIQUE IMPORT LABEL` {`childPid`:1912, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt9\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562869149, `lamportLogicalTime`:9, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":3,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":4,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":3}\", `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147978004, `UNIQUE IMPORT ID`:32817});");
                tx.run("CREATE (:`END`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt14\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039563021498, `lamportLogicalTime`:7, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":4,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":2,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1900, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978007, `UNIQUE IMPORT ID`:32820});");
                tx.run("CREATE (:`JOIN`:`UNIQUE IMPORT LABEL` {`childPid`:1911, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt13\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039563021498, `lamportLogicalTime`:10, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":4,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":4,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":4}\", `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147978007, `UNIQUE IMPORT ID`:32821});");
                tx.run("CREATE (:`CONNECT`:`UNIQUE IMPORT LABEL` {`comm`:\"client\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt5\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562069883, `lamportLogicalTime`:4, `vectorLogicalTime`:\"{\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":2,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1912, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:37978, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:37978, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147977998, `UNIQUE IMPORT ID`:32824});");
                tx.run("CREATE (:`RCV`:`UNIQUE IMPORT LABEL` {`comm`:\"client\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt6\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562181777, `lamportLogicalTime`:7, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":3,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":3,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1912, `size`:26, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:5000, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:5000, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147978000, `UNIQUE IMPORT ID`:32825});");
                tx.run("CREATE (:`ACCEPT`:`UNIQUE IMPORT LABEL` {`comm`:\"server\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt7\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562208729, `lamportLogicalTime`:5, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":2,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":2,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1911, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:37978, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:37978, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978002, `UNIQUE IMPORT ID`:32826});");
                tx.run("CREATE (:`SND`:`UNIQUE IMPORT LABEL` {`comm`:\"server\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt8\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562412921, `lamportLogicalTime`:6, `vectorLogicalTime`:\"{\\\"1911@cloud83.cluster.lsd.di.uminho.pt\\\":3,\\\"1912@cloud83.cluster.lsd.di.uminho.pt\\\":2,\\\"1900@cloud83.cluster.lsd.di.uminho.pt\\\":2}\", `pid`:1911, `size`:26, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:5000, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:5000, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978003, `UNIQUE IMPORT ID`:32827});");
                return null;
            }
        });

        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;");
                return null;
            }
        });

        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32812}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32813}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32812}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32814}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32814}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32815}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32814}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32817}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32816}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32817}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32820}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32821}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32817}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32821}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32815}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32824}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32824}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32825}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32825}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32816}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32813}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32826}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32824}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32826}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32826}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32827}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32827}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32820}) CREATE (n1)-[r:`HAPPENS_BEFORE`]->(n2);");
                tx.run("MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32827}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:32825}) CREATE (n1)-[r:`HAPPENS_BEFORE` {`size`:26}]->(n2);");
                return null;
            }
        });

        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;");
                return null;
            }
        });

        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;");
                return null;
            }
        });

        session.run("MATCH (n) SET n :EVENT");
    }

    public static void testResult(GraphDatabaseService db, String call, Consumer<Result> resultConsumer) {
        testResult(db, call, null, resultConsumer);
    }

    public static void testResult(GraphDatabaseService db, String call, Map<String, Object> params, Consumer<Result> resultConsumer) {
        try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
            Map<String, Object> p = (params == null) ? Collections.emptyMap() : params;
            resultConsumer.accept(db.execute(call, p));
            tx.success();
        }
    }
}
