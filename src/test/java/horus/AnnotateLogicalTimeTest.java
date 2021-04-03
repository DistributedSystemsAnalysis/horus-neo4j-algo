package horus;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class AnnotateLogicalTimeTest {
    private static final String CLIENT_SERVER_WITH_UNRELATED_TIMELINES = "CREATE " +
            "(a1:EVENT:CREATE {threadId:'1910', eventId:'1@cloud83', kernelTime: 1}), " +
            "(a2:EVENT:CREATE {threadId:'1910', eventId:'2@cloud83', kernelTime: 2}), " +
            "(a3:EVENT:JOIN {threadId:'1910', eventId:'3@cloud83', kernelTime: 3}), " +
            "(a4:EVENT:JOIN {threadId:'1910', eventId:'4@cloud83', kernelTime: 4}), " +

            "(b1:EVENT:START {threadId:'1911', eventId:'5@cloud83', kernelTime: 5}), " +
            "(b2:EVENT:ACCEPT {threadId:'1911', eventId:'6@cloud83', kernelTime: 6}), " +
            "(b3:EVENT:SND {threadId:'1911', eventId:'7@cloud83', kernelTime: 7}), " +
            "(b4:EVENT:END {threadId:'1911', eventId:'8@cloud83', kernelTime: 8}), " +

            "(c1:EVENT:START {threadId:'1912', eventId:'9@cloud83', kernelTime: 9}), " +
            "(c2:EVENT:CONNECT {threadId:'1912', eventId:'10@cloud83', kernelTime: 10}), " +
            "(c3:EVENT:RCV {threadId:'1912', eventId:'11@cloud83', kernelTime: 11}), " +
            "(c4:EVENT:END {threadId:'1912', eventId:'12@cloud83', kernelTime: 12}), " +

            "(d1:EVENT:SND {threadId:'1913', eventId:'13@cloud83', kernelTime: 13}), " +

            "(a1)-[:HAPPENS_BEFORE]->(a2)-[:HAPPENS_BEFORE]->(a3)-[:HAPPENS_BEFORE]->(a4), " +
            "(b1)-[:HAPPENS_BEFORE]->(b2)-[:HAPPENS_BEFORE]->(b3)-[:HAPPENS_BEFORE]->(b4), " +
            "(c1)-[:HAPPENS_BEFORE]->(c2)-[:HAPPENS_BEFORE]->(c3)-[:HAPPENS_BEFORE]->(c4), " +

            "(a1)-[:HAPPENS_BEFORE]->(b1), " +
            "(a2)-[:HAPPENS_BEFORE]->(c1), " +
            "(b3)-[:HAPPENS_BEFORE]->(c3), " +
            "(b4)-[:HAPPENS_BEFORE]->(a4), " +
            "(c2)-[:HAPPENS_BEFORE]->(b2), " +
            "(c4)-[:HAPPENS_BEFORE]->(a3)";

    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure(AnnotateLogicalTime.class);

    @Test
    public void calculatesLogicalTime() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryption().toConfig());
             Session session = driver.session()) {

            session.run(CLIENT_SERVER_WITH_UNRELATED_TIMELINES).consume();

            session.run("CALL horus.annotateLogicalTime()").consume();

            Record node;
            StatementResult result;

            result = session.run("MATCH (n:EVENT {eventId: '1@cloud83'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(1, vc.getTime("1910").intValue());
                    assertEquals(0, vc.getTime("1911").intValue());
                    assertEquals(0, vc.getTime("1912").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(1), lc));

            result = session.run("MATCH (n:EVENT {eventId: '12@cloud83'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(2, vc.getTime("1910").intValue());
                    assertEquals(3, vc.getTime("1911").intValue());
                    assertEquals(4, vc.getTime("1912").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(8), lc));

            result = session.run("MATCH (n:EVENT {eventId: '4@cloud83'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(4, vc.getTime("1910").intValue());
                    assertEquals(4, vc.getTime("1911").intValue());
                    assertEquals(4, vc.getTime("1912").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(10), lc));

            result = session.run("MATCH (n:EVENT {eventId: '13@cloud83'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(0, vc.getTime("1910").intValue());
                    assertEquals(0, vc.getTime("1911").intValue());
                    assertEquals(0, vc.getTime("1912").intValue());
                    assertEquals(1, vc.getTime("1913").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(1), lc));
        }
    }

    @Test
    public void calculatesLogicalTimeInRealExample() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryption().toConfig());
             Session session = driver.session()) {

            this.createServerClientExample(session);

            session.run("CALL horus.annotateLogicalTime()").consume();

            Record node;
            StatementResult result;

            result = session.run("MATCH (n {eventId: 'cloud83.cluster.lsd.di.uminho.pt1'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(1, vc.getTime("1900@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(0, vc.getTime("1911@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(0, vc.getTime("1912@cloud83.cluster.lsd.di.uminho.pt").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(1), lc));

            result = session.run("MATCH (n {eventId: 'cloud83.cluster.lsd.di.uminho.pt10'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(2, vc.getTime("1900@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(3, vc.getTime("1911@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(4, vc.getTime("1912@cloud83.cluster.lsd.di.uminho.pt").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(8), lc));


            result = session.run("MATCH (n {eventId: 'cloud83.cluster.lsd.di.uminho.pt13'}) RETURN n.threadId as threadId, n.vectorLogicalTime as vectorLogicalTime, n.lamportLogicalTime as lamportLogicalTime");

            assertNotNull(node = result.single());
            testVectorLogicalTime(node, new Consumer<VectorClock>() {
                @Override
                public void accept(VectorClock vc) {
                    assertEquals(4, vc.getTime("1900@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(4, vc.getTime("1911@cloud83.cluster.lsd.di.uminho.pt").intValue());
                    assertEquals(4, vc.getTime("1912@cloud83.cluster.lsd.di.uminho.pt").intValue());
                }
            });
            testLamportLogicalTime(node, lc -> assertEquals(new Long(10), lc));
        }
    }


    public static void testVectorLogicalTime(Record record, Consumer<VectorClock> timeConsumer) throws IOException {
        String vectorLogicalTimeJson = record.get("vectorLogicalTime").asString();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode time = objectMapper.readTree(vectorLogicalTimeJson);

        Map<String, Integer> timeMap = new HashMap<>();
        time.getFields().forEachRemaining(new Consumer<Map.Entry<String, JsonNode>>() {
            @Override
            public void accept(Map.Entry<String, JsonNode> entry) {
                if (!entry.getValue().isInt())
                    fail("Unexpected values in vectorLogicalTime field.");

                timeMap.put(entry.getKey(), entry.getValue().getIntValue());
            }
        });

        VectorClock vectorLogicalTime = new VectorClock(record.get("threadId").asString(), timeMap);

        timeConsumer.accept(vectorLogicalTime);
    }

    public static void testLamportLogicalTime(Record record, Consumer<Long> timeConsumer) throws IOException {
        timeConsumer.accept(record.get("lamportLogicalTime").asLong());
    }

    private void createServerClientExample(Session session) {
        session.writeTransaction(new TransactionWork<Object>() {
            @Override
            public Object execute(Transaction tx) {
                tx.run("CREATE (:`CREATE`:`UNIQUE IMPORT LABEL` {`childPid`:1911, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt1\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561205963, `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32812});");
                tx.run("CREATE (:`START`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt2\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561205963, `pid`:1900, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32813});");
                tx.run("CREATE (:`CREATE`:`UNIQUE IMPORT LABEL` {`childPid`:1912, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt3\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561239389, `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147976997, `UNIQUE IMPORT ID`:32814});");
                tx.run("CREATE (:`START`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt4\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259038561239389, `pid`:1900, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147976998, `UNIQUE IMPORT ID`:32815});");
                tx.run("CREATE (:`END`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt10\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562869149, `pid`:1900, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147978004, `UNIQUE IMPORT ID`:32816});");
                tx.run("CREATE (:`JOIN`:`UNIQUE IMPORT LABEL` {`childPid`:1912, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt9\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562869149, `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147978004, `UNIQUE IMPORT ID`:32817});");
                tx.run("CREATE (:`END`:`UNIQUE IMPORT LABEL` {`comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt14\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039563021498, `pid`:1900, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978007, `UNIQUE IMPORT ID`:32820});");
                tx.run("CREATE (:`JOIN`:`UNIQUE IMPORT LABEL` {`childPid`:1911, `comm`:\"driver\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt13\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039563021498, `pid`:1900, `threadId`:\"1900@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1900, `userTime`:1548147978007, `UNIQUE IMPORT ID`:32821});");
                tx.run("CREATE (:`CONNECT`:`UNIQUE IMPORT LABEL` {`comm`:\"client\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt5\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562069883, `pid`:1912, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:37978, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:37978, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147977998, `UNIQUE IMPORT ID`:32824});");
                tx.run("CREATE (:`RCV`:`UNIQUE IMPORT LABEL` {`comm`:\"client\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt6\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562181777, `pid`:1912, `size`:26, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:5000, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:5000, `threadId`:\"1912@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1912, `userTime`:1548147978000, `UNIQUE IMPORT ID`:32825});");
                tx.run("CREATE (:`ACCEPT`:`UNIQUE IMPORT LABEL` {`comm`:\"server\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt7\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562208729, `pid`:1911, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:37978, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:37978, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978002, `UNIQUE IMPORT ID`:32826});");
                tx.run("CREATE (:`SND`:`UNIQUE IMPORT LABEL` {`comm`:\"server\", `eventId`:\"cloud83.cluster.lsd.di.uminho.pt8\", `host`:\"cloud83.cluster.lsd.di.uminho.pt\", `kernelTime`:259039562412921, `pid`:1911, `size`:26, `socketFamily`:2, `socketFrom`:\"127.0.0.1\", `socketFromPort`:5000, `socketId`:\"127.0.0.1:5000-127.0.0.1:37978\", `socketTo`:\"127.0.0.1\", `socketToPort`:5000, `threadId`:\"1911@cloud83.cluster.lsd.di.uminho.pt\", `tid`:1911, `userTime`:1548147978003, `UNIQUE IMPORT ID`:32827});");
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
}
