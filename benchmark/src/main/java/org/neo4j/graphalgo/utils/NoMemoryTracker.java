package org.neo4j.graphalgo.utils;

import org.neo4j.memory.MemoryAllocationTracker;

final class NoMemoryTracker implements MemoryAllocationTracker {

    static final MemoryAllocationTracker INSTANCE = new NoMemoryTracker();

    @Override
    public void allocated(final long bytes) {
    }

    @Override
    public void deallocated(final long bytes) {
    }

    @Override
    public long usedDirectMemory() {
        return 0L;
    }
}
