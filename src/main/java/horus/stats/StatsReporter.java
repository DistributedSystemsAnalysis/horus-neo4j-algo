package horus.stats;

import java.util.Timer;
import java.util.TimerTask;

public class StatsReporter {
    Timer timer;

    public StatsReporter(ExecutionStats stats, int intervalSeconds) {
        timer = new Timer();
        timer.schedule(new ReportTask(stats), 0, intervalSeconds * 1000);
    }

    class ReportTask extends TimerTask {
        private ExecutionStats stats;

        public ReportTask(ExecutionStats stats) {
            this.stats = stats;
        }

        @Override
        public void run() {
            this.stats.printStats();
        }
    }
}