package com.dataflow.engine;

import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
public class Partitioner {
    public <T> List<List<T>> partition(List<T> items, int requestedPartitions) {
        if (requestedPartitions < 1) throw new IllegalArgumentException("Partition count must be at least 1");
        int count = Math.min(requestedPartitions, Math.max(1, items.size()));
        List<List<T>> partitions = new ArrayList<>(count);
        int baseSize = items.size() / count;
        int remainder = items.size() % count;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            int size = baseSize + (i < remainder ? 1 : 0);
            partitions.add(List.copyOf(items.subList(offset, offset + size)));
            offset += size;
        }
        return partitions;
    }
}
