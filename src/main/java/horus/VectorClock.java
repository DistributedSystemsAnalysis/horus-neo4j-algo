package horus;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VectorClock {

    private String processId;
    private HashMap<String, MutableInt> logicalTime;

    public VectorClock(String myProcessId) {
        processId = myProcessId;
        logicalTime = new HashMap<>();
    }

    public VectorClock(String myProcessId, Map<String, Integer> processTime) {
        this(myProcessId);

        for (String processId : processTime.keySet()) {
            logicalTime.put(processId, new MutableInt(processTime.get(processId).intValue()));
        }
    }

    private Set<String> getProcessIds() {
        return this.logicalTime.keySet();
    }

    public VectorClock increment() {
        this.logicalTime.putIfAbsent(this.processId, new MutableInt(0));

        this.logicalTime.get(this.processId).increment();

        return this;
    }

    public boolean lessThan(VectorClock vc) {
        boolean foundLess = false;
        Set<String> processIds = new HashSet<>();
        processIds.addAll(this.getProcessIds());
        processIds.addAll(vc.getProcessIds());

        for (String processId : processIds) {
            int myTime = this.getTime(processId);
            int otherTime = vc.getTime(processId);

            if (myTime > otherTime)
                return false;

            foundLess = foundLess ? foundLess : myTime < otherTime;
        }

        return foundLess;
    }

    public boolean withinCausalPath(VectorClock from, VectorClock to) {
        if (this.equals(from) || this.equals(to))
            return true;

        return from.lessThan(this) && this.lessThan(to);
    }

    public VectorClock merge(VectorClock vc) {
        this.doMerge(vc);

        this.increment();

        return this;
    }

    public VectorClock mergeWithoutIncrement(VectorClock vc) {
        this.doMerge(vc);

        return this;
    }

    public boolean equals(VectorClock vc) {
        if (vc == this)
            return true;

        Set<String> processIds = new HashSet<>();
        processIds.addAll(this.getProcessIds());
        processIds.addAll(vc.getProcessIds());

        for (String processId : processIds) {
            int myTime = this.getTime(processId);
            int otherTime = vc.getTime(processId);

            if (myTime != otherTime)
                return false;
        }

        return true;
    }


    private void doMerge(VectorClock vc) {
        int currentClock;
        Set<String> processIds = new HashSet<>();
        processIds.addAll(this.getProcessIds());
        processIds.addAll(vc.getProcessIds());

        for (String processId : processIds) {
            if (this.getTime(processId) < (currentClock = vc.getTime(processId)))
                this.logicalTime.put(processId, new MutableInt(currentClock));
        }
    }

    public Integer getTime(String processId) {
        if (this.logicalTime.containsKey(processId))
            return this.logicalTime.get(processId).intValue();

        return 0;
    }

    private MutableInt getMutableTime(String processId) {
        return this.logicalTime.get(processId);
    }

    public Map<String, Integer> toMap() {
        Map<String, Integer> currentTime = new HashMap<>();

        for (String threadId : this.logicalTime.keySet()) {
            currentTime.put(threadId, this.logicalTime.get(threadId).intValue());
        }

        return currentTime;
    }

    @Override
    public String toString() {
        return "VectorClock{" +
                "processId='" + processId + '\'' +
                ", logicalTime=" + logicalTime +
                '}';
    }
}
