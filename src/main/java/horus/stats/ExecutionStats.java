package horus.stats;

import java.util.HashMap;
import java.util.Map;

class OperationStats {
    public long nOps = 0;
    public long elapsedTime = 0;

    @Override
    public String toString() {
        double throughput = nOps / (elapsedTime / 1000.0);
        double latency = elapsedTime / (double) nOps;

        return "{" +
                "nOps=" + nOps +
                ", elapsedTime(ms)=" + elapsedTime +
                ", ops/s=" + throughput +
                ", ms/op=" + latency +
                '}';
    }
}

public class ExecutionStats {

    private static ExecutionStats instance = null;

    private HashMap<String, OperationStats> stats;

    public static ExecutionStats getInstance() {
        if (instance == null) {
            instance = new ExecutionStats();
        }

        return instance;
    }

    public ExecutionStats() {
        this.stats = new HashMap<>();
    }

    public void measure(String opName, long millis) {
        OperationStats stats = this.stats.getOrDefault(opName, new OperationStats());
        stats.nOps++;
        stats.elapsedTime += millis;

        this.stats.put(opName, stats);
    }

    public void printStats() {
        long totalOps = 0;
        long totalOElapsedTime = 0;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("-----------\n");
        for (Map.Entry<String, OperationStats> entry : this.stats.entrySet()) {
            stringBuilder.append(entry.getKey()).append(": ").append(entry.getValue().toString()).append("\n");
            totalOps += entry.getValue().nOps;
            totalOElapsedTime += entry.getValue().elapsedTime;
        }
        stringBuilder.append("-----------\n");
        stringBuilder.append("total ops: ").append(totalOps).append(", total ms: ").append(totalOElapsedTime).append(" \n");
        stringBuilder.append("-----------\n");
        System.out.print(stringBuilder.toString());

    }
}
