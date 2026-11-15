package com.dataflow.engine;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import static org.junit.jupiter.api.Assertions.*;

class DataFlowEngineTest {
    @Test void aggregatesCountsAcrossParallelPartitions() {
        var engine = new DataFlowEngine(new Partitioner());
        try {
            var rows = List.of(Map.of("city", "Pune"), Map.of("city", "Delhi"),
                    Map.of("city", "Pune"), Map.of("city", "Kolkata"), Map.of("city", "Delhi"));
            assertEquals(Map.of("Pune", 2L, "Delhi", 2L, "Kolkata", 1L), engine.countByColumn(rows, "city", 3));
        } finally { engine.close(); }
    }

    @Test void rejectsWorkAfterShutdownWithoutLeavingAnIncompleteFuture() {
        var engine = new DataFlowEngine(new Partitioner());
        engine.close();

        assertThrows(CompletionException.class, () -> engine.countByColumn(
                List.of(Map.of("city", "Pune")), "city", 1));
    }
}
