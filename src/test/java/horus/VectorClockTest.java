package horus;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class VectorClockTest {

    @Test
    public void increment() {
        VectorClock clock = new VectorClock("myid");
        clock.increment();

        assertEquals(1, clock.getTime("myid").intValue());
        assertEquals(0, clock.getTime("otherid").intValue());
    }

    @Test
    public void lessThan() {
        Map<String, Integer> processesTime = new HashMap<String, Integer>() {{
            put("myid", 0);
            put("pid1", 0);
            put("pid2", 2);
        }};

        VectorClock lowerClock = new VectorClock("myid", processesTime);

        processesTime = new HashMap<String, Integer>() {{
            put("myid", 1);
            put("pid1", 3);
            put("pid2", 2);
        }};

        VectorClock higherClock = new VectorClock("myid", processesTime);

        assertTrue(lowerClock.lessThan(higherClock));
        assertFalse(higherClock.lessThan(lowerClock));
    }

    @Test
    public void lessThanOnClocksWithSameTimeOnaSinglThread() {
        Map<String, Integer> processesTime = new HashMap<String, Integer>() {{
            put("myid", 2);
            put("pid1", 0);
            put("pid2", 0);
        }};

        VectorClock lowerClock = new VectorClock("myid", processesTime);

        processesTime = new HashMap<String, Integer>() {{
            put("myid", 2);
            put("pid1", 3);
            put("pid2", 4);
        }};

        VectorClock higherClock = new VectorClock("myid", processesTime);

        assertTrue(lowerClock.lessThan(higherClock));
        assertFalse(higherClock.lessThan(lowerClock));
    }

    @Test
    public void lessThanOnClocksWithSameTime() {
        Map<String, Integer> processesTime = new HashMap<String, Integer>() {{
            put("myid", 2);
            put("pid1", 0);
            put("pid2", 0);
        }};

        VectorClock lowerClock = new VectorClock("myid", processesTime);

        processesTime = new HashMap<String, Integer>() {{
            put("myid", 2);
            put("pid1", 0);
            put("pid2", 0);
        }};

        VectorClock higherClock = new VectorClock("myid", processesTime);

        assertFalse(lowerClock.lessThan(higherClock));
    }

    @Test
    public void mergeAndIncrement() {
        VectorClock currentClock = new VectorClock("pid2", new HashMap<String, Integer>() {{
            put("pid1", 0);
            put("pid2", 0);
            put("pid3", 2);
        }});

        VectorClock parentClock = new VectorClock("pid3", new HashMap<String, Integer>() {{
            put("pid1", 1);
            put("pid2", 2);
            put("pid3", 0);
        }});

        currentClock.merge(parentClock);

        assertTrue(parentClock.lessThan(currentClock));
        assertEquals(1, currentClock.getTime("pid1").intValue());
        assertEquals(3, currentClock.getTime("pid2").intValue());
        assertEquals(2, currentClock.getTime("pid3").intValue());
    }

    @Test
    public void mergeVectorClocksWithoutAllProcesses() {
        VectorClock currentClock = new VectorClock("pid2", new HashMap<String, Integer>() {{
            put("pid2", 0);
            put("pid3", 2);
        }});

        VectorClock parentClock = new VectorClock("pid3", new HashMap<String, Integer>() {{
            put("pid1", 1);
            put("pid2", 2);
        }});

        currentClock.merge(parentClock);

        assertTrue(parentClock.lessThan(currentClock));
        assertEquals(1, currentClock.getTime("pid1").intValue());
        assertEquals(3, currentClock.getTime("pid2").intValue());
        assertEquals(2, currentClock.getTime("pid3").intValue());
    }

    @Test
    public void equals() {
        VectorClock clock1 = new VectorClock("pid2", new HashMap<String, Integer>() {{
            put("pid2", 2);
            put("pid3", 0);
        }});

        VectorClock clock2 = new VectorClock("pid3", new HashMap<String, Integer>() {{
            put("pid2", 2);
            put("pid1", 0);
        }});

        VectorClock clock3 = new VectorClock("pid3", new HashMap<String, Integer>() {{
            put("pid1", 1);
            put("pid2", 3);
        }});

        assertTrue(clock1.equals(clock1));
        assertTrue(clock1.equals(clock2));
        assertFalse(clock1.equals(clock3));
        assertFalse(clock2.equals(clock3));
    }
}