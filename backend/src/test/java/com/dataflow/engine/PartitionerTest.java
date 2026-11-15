package com.dataflow.engine;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PartitionerTest {
    private final Partitioner partitioner = new Partitioner();
    @Test void partitionsAllItemsWithoutLoss() {
        var parts = partitioner.partition(List.of(1,2,3,4,5,6,7), 3);
        assertEquals(3, parts.size());
        assertEquals(List.of(1,2,3,4,5,6,7), parts.stream().flatMap(List::stream).toList());
        assertEquals(3, parts.get(0).size());
        assertEquals(2, parts.get(1).size());
    }
    @Test void rejectsZeroPartitions() {
        assertThrows(IllegalArgumentException.class, () -> partitioner.partition(List.of(1), 0));
    }
    @Test void emptyInputStillHasOneEmptyPartition() {
        assertEquals(List.of(List.of()), partitioner.partition(List.of(), 4));
    }
}
