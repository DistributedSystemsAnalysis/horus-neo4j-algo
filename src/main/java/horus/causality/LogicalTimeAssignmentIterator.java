package horus.causality;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import horus.VectorClock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.neo4j.graphdb.Node;

import java.util.HashMap;

public class LogicalTimeAssignmentIterator extends BreadthFirstIterator<CausalNode, DefaultEdge> {
    private HashMap<CausalNode, MutableInt> parentsWithLogicalTime;

    public LogicalTimeAssignmentIterator(Graph g) {
        super(g);
        this.parentsWithLogicalTime = new HashMap<>();
    }

    public LogicalTimeAssignmentIterator(Graph g, CausalNode startVertex) {
        super(g, startVertex);
        this.parentsWithLogicalTime = new HashMap<>();
    }

    public LogicalTimeAssignmentIterator(Graph g, Iterable<CausalNode> startVertices) {
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
            updatedLogicalTime.merge(currentParentLogicalTime);

        } else {
            updatedLogicalTime.mergeWithoutIncrement(currentParentLogicalTime);
            continuePath = false;
        }

        vertex.setVectorClock(updatedLogicalTime);

        if (continuePath)
            super.encounterVertex(vertex, edge);
    }

    protected void encounterVertexAgain(CausalNode vertex, DefaultEdge edge) {
        this.encounterVertex(vertex, edge);
    }
}
