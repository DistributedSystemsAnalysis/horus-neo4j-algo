package horus.causality;

import apoc.result.VirtualNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import horus.VectorClock;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CausalNode {
    private Node node;
    private VectorClock vectorClock;

    public CausalNode(Node node) {
        this.node = node;
    }

    public VectorClock getVectorClock() {
        if (this.vectorClock != null) {
            return this.vectorClock;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode time = null;
        try {
            time = objectMapper.readTree((String) this.node.getProperty("vectorLogicalTime"));
        } catch (IOException | NullPointerException e) {
            return null;
        }

        Map<String, Integer> timeMap = new HashMap<>();
        time.fields().forEachRemaining(entry -> {
            if (!entry.getValue().isInt())
                throw new RuntimeException("Unexpected values in vectorLogicalTime field.");

            timeMap.put(entry.getKey(), entry.getValue().intValue());
        });

        return this.vectorClock = new VectorClock((String) this.node.getProperty("threadId"), timeMap);
    }

    public void setVectorClock(VectorClock vectorClock) {
        try {
            this.node.setProperty("vectorLogicalTime", (new ObjectMapper()).writeValueAsString(vectorClock.toMap()));
            this.vectorClock = vectorClock;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not set vector clock. " + e.getMessage());
        }
    }

    public Node getNode() {
        return this.node;
    }

    public String getTimelineId() {
        return (String) this.node.getProperty("threadId");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CausalNode that = (CausalNode) o;

        return Objects.equals(this.node, that.node);
    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }
}
