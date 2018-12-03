package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

import java.util.concurrent.atomic.AtomicLong;

final class HugeIdMapBuilder {

    private final HugeLongArray array;
    private final long numberOfNodes;
    private final AllocationTracker tracker;
    private final AtomicLong allocationIndex;
    private final ThreadLocal<BulkAdder> adders;

    static HugeIdMapBuilder of(long numberOfNodes, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(numberOfNodes, tracker);
        return new HugeIdMapBuilder(array,  numberOfNodes, tracker);
    }

    private HugeIdMapBuilder(HugeLongArray array, final long numberOfNodes, AllocationTracker tracker) {
        this.array = array;
        this.numberOfNodes = numberOfNodes;
        this.tracker = tracker;
        this.allocationIndex = new AtomicLong();
        this.adders = ThreadLocal.withInitial(() -> new BulkAdder(array, array.newCursor()));
    }

    BulkAdder allocate(final long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == numberOfNodes) {
            return null;
        }
        BulkAdder adder = adders.get();
        adder.reset(startIndex, upperAllocation(startIndex, nodes));
        return adder;
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(numberOfNodes, lower + nodes);
    }

    HugeIdMap build(long highestNodeId) {
        HugeLongArray graphIds = array;
        SparseLongArray nodeToGraphIds = SparseLongArray.newArray(highestNodeId, tracker);

        try (HugeLongArray.Cursor cursor = graphIds.cursor(adders.get().cursor)) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                long internalId = cursor.base + offset;
                for (int i = offset; i < limit; ++i, ++internalId) {
                    nodeToGraphIds.set(array[i], internalId);
                }
            }
        }
        adders.remove();

        return new HugeIdMap(graphIds, nodeToGraphIds, allocationIndex.get());
    }

    final class BulkAdder {
        long[] buffer;
        int offset;
        int length;

        private final HugeLongArray array;
        private final HugeLongArray.Cursor cursor;

        BulkAdder(
                HugeLongArray array,
                HugeLongArray.Cursor cursor) {
            this.array = array;
            this.cursor = cursor;
        }

        private void reset(long start, long end) {
            array.cursor(this.cursor, start, end);
        }

        boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }
    }
}
