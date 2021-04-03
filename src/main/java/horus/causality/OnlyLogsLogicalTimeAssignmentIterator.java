package horus.causality;

import horus.VectorClock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnlyLogsLogicalTimeAssignmentIterator extends BreadthFirstIterator<CausalNode, DefaultEdge> {
    private HashMap<CausalNode, MutableInt> parentsWithLogicalTime;
    private HashMap<String, List<String>> filterKeyValue = null;

    public OnlyLogsLogicalTimeAssignmentIterator(Graph g) {
        super(g);
        this.parentsWithLogicalTime = new HashMap<>();

    }

    public OnlyLogsLogicalTimeAssignmentIterator(Graph g, CausalNode startVertex, List<String> filterHosts) {
        super(g, startVertex);
        this.parentsWithLogicalTime = new HashMap<>();

        this.filterKeyValue = new HashMap<>();
        this.filterKeyValue.put("host", filterHosts);
    }

    public OnlyLogsLogicalTimeAssignmentIterator(Graph g, Iterable<CausalNode> startVertices) {
        super(g, startVertices);
        this.parentsWithLogicalTime = new HashMap<>();
    }

    protected void encounterVertex(CausalNode vertex, DefaultEdge edge) {
        String timelineId = vertex.getTimelineId();
        CausalNode currentParent = edge == null ? null : this.graph.getEdgeSource(edge);

        if (currentParent == null) {
            VectorClock vc = new VectorClock(timelineId);
            vertex.setVectorClock(vc.increment());

            super.encounterVertex(vertex, edge);

            return;
        }

        this.parentsWithLogicalTime.putIfAbsent(vertex, new MutableInt(0));

        int totalParents = this.graph.inDegreeOf(vertex);
        int totalParentsWithLogicalTime = this.parentsWithLogicalTime.get(vertex).incrementAndGet();
        VectorClock currentLogicalTime = vertex.getVectorClock();
        VectorClock currentParentLogicalTime = currentParent.getVectorClock();

        VectorClock updatedLogicalTime = null;
        if (currentLogicalTime != null) {
            updatedLogicalTime = vertex.getVectorClock();
        } else {
            updatedLogicalTime = new VectorClock(timelineId);
        }

        boolean continuePath = true;
        if (totalParentsWithLogicalTime == totalParents) {
            this.parentsWithLogicalTime.remove(vertex);

            if (vertex.getNode().hasLabel(Label.label("LOG")) && this.shouldIncludeVertex(vertex)) {
                updatedLogicalTime.merge(currentParentLogicalTime);
            } else {
                updatedLogicalTime.mergeWithoutIncrement(currentParentLogicalTime);
            }
        } else {
            updatedLogicalTime.mergeWithoutIncrement(currentParentLogicalTime);
            continuePath = false;
        }

        vertex.setVectorClock(updatedLogicalTime);

        if (continuePath)
            super.encounterVertex(vertex, edge);
    }

    protected boolean shouldIncludeVertex(CausalNode vertex) {
        for (Map.Entry<String, List<String>> property : this.filterKeyValue.entrySet()) {
            if (property.getValue().contains((String) vertex.getNode().getProperty(property.getKey(), null)))
                return true;
        }

        return false;
    }

    protected void encounterVertexAgain(CausalNode vertex, DefaultEdge edge) {
        this.encounterVertex(vertex, edge);
    }
}
