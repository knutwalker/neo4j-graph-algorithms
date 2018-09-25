package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class AdjacencyBuilder {

    @FunctionalInterface
    interface AdjacencyImporter {
        int add(int pageIndex, HugeAdjacencyBuilder builder, int localId);
    }

    @FunctionalInterface
    interface SingleAdjacencyImporter {
        void add(
                int localId,
                long targetId,
                int degree,
                HugeAdjacencyBuilder adjacency,
                CompressedLongArray[] targets,
                LongsRef buffer);
    }

    abstract void addAdjacencyImporter(
            AllocationTracker tracker,
            boolean loadDegrees,
            int pageIndex);

    abstract void finishPreparation();

    abstract int add(
            AdjacencyImporter importer,
            long nodeId);

    abstract void add(
            long nodeId,
            long targetId,
            SingleAdjacencyImporter importer);

    static AdjacencyBuilder of(
            HugeAdjacencyBuilder adjacency,
            int numPages,
            int pageSize,
            AllocationTracker tracker) {
        if (adjacency == null) {
            return NoAdjacency.INSTANCE;
        }
        if (numPages == 1) {
            return new SinglePagedAdjacency(adjacency, pageSize);
        }
        tracker.add(sizeOfObjectArray(numPages) << 1);
        HugeAdjacencyBuilder[] builders = new HugeAdjacencyBuilder[numPages];
        long[][] degrees = new long[numPages][];
        return new PagedAdjacency(adjacency, builders, degrees, pageSize);
    }

    static AdjacencyBuilder compressing(
            HugeAdjacencyBuilder adjacency,
            int numPages,
            int pageSize,
            AllocationTracker tracker) {
        if (adjacency == null) {
            return NoAdjacency.INSTANCE;
        }
        tracker.add(sizeOfObjectArray(numPages) << 2);
        HugeAdjacencyBuilder[] builders = new HugeAdjacencyBuilder[numPages];
        final CompressedLongArray[][] targets = new CompressedLongArray[numPages][];
        LongsRef[] buffers = new LongsRef[numPages];
        long[][] degrees = new long[numPages][];
        return new CompressingPagedAdjacency(adjacency, builders, targets, buffers, degrees, pageSize);
    }

    private static final class PagedAdjacency extends AdjacencyBuilder {

        private final HugeAdjacencyBuilder adjacency;
        private final HugeAdjacencyBuilder[] builders;
        private final long[][] degrees;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;

        private PagedAdjacency(
                HugeAdjacencyBuilder adjacency,
                HugeAdjacencyBuilder[] builders,
                long[][] degrees,
                int pageSize) {
            this.adjacency = adjacency;
            this.builders = builders;
            this.degrees = degrees;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
        }

        @Override
        void addAdjacencyImporter(AllocationTracker tracker, boolean loadDegrees, int pageIndex) {
            tracker.add(sizeOfLongArray(pageSize));
            long[] offsets = degrees[pageIndex] = new long[pageSize];
            builders[pageIndex] = adjacency.threadLocalCopy(offsets, loadDegrees);
            builders[pageIndex].prepare();
        }

        @Override
        void finishPreparation() {
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, pageSize));
        }

        @Override
        int add(AdjacencyImporter importer, long nodeId) {
            int pageIndex = (int) (nodeId >>> pageShift);
            return importer.add(
                    pageIndex,
                    builders[pageIndex],
                    (int) (nodeId & pageMask)
            );
        }

        @Override
        void add(long nodeId, long targetId, SingleAdjacencyImporter importer) {
        }
    }

    private static final class CompressingPagedAdjacency extends AdjacencyBuilder {

        private final HugeAdjacencyBuilder adjacency;
        private final HugeAdjacencyBuilder[] builders;
        private final CompressedLongArray[][] targets;
        private final LongsRef[] buffers;
        private final long[][] degrees;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;
        private final long sizeOfLongPage;
        private final long sizeOfObjectPage;

        private CompressingPagedAdjacency(
                HugeAdjacencyBuilder adjacency,
                HugeAdjacencyBuilder[] builders,
                CompressedLongArray[][] targets,
                LongsRef[] buffers,
                long[][] degrees,
                int pageSize) {
            this.adjacency = adjacency;
            this.builders = builders;
            this.targets = targets;
            this.buffers = buffers;
            this.degrees = degrees;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
            sizeOfLongPage = sizeOfLongArray(pageSize);
            sizeOfObjectPage = sizeOfObjectArray(pageSize);
        }

        @Override
        void addAdjacencyImporter(AllocationTracker tracker, boolean loadDegrees, int pageIndex) {
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfLongPage);
            targets[pageIndex] = new CompressedLongArray[pageSize];
            buffers[pageIndex] = new LongsRef();
            long[] offsets = degrees[pageIndex] = new long[pageSize];
            builders[pageIndex] = adjacency.threadLocalCopy(offsets, loadDegrees);
            builders[pageIndex].prepare();
        }

        @Override
        void finishPreparation() {
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, pageSize));
        }

        @Override
        int add(AdjacencyImporter importer, long nodeId) {
            return 0;
        }

        @Override
        void add(long nodeId, long targetId, SingleAdjacencyImporter importer) {
            int pageIndex = (int) (nodeId >>> pageShift);
            int localId = (int) (nodeId & pageMask);
            HugeAdjacencyBuilder adjacency = builders[pageIndex];
            synchronized (adjacency) {
                importer.add(
                        localId,
                        targetId,
                        adjacency.degree(localId),
                        adjacency,
                        targets[pageIndex],
                        buffers[pageIndex]);
            }
        }
    }

    private static final class SinglePagedAdjacency extends AdjacencyBuilder {

        private final HugeAdjacencyBuilder adjacency;
        private final int numberOfNodes;
        private HugeAdjacencyBuilder builder;
        private long[] degrees;

        private SinglePagedAdjacency(
                HugeAdjacencyBuilder adjacency,
                int numberOfNodes) {
            this.adjacency = adjacency;
            this.numberOfNodes = numberOfNodes;
        }

        @Override
        void addAdjacencyImporter(AllocationTracker tracker, boolean loadDegrees, int pageIndex) {
            assert pageIndex == 0;
            tracker.add(sizeOfLongArray(numberOfNodes));
            long[] offsets = degrees = new long[numberOfNodes];
            builder = adjacency.threadLocalCopy(offsets, loadDegrees);
            builder.prepare();
        }

        @Override
        void finishPreparation() {
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees));
        }

        @Override
        int add(AdjacencyImporter importer, long nodeId) {
            assert nodeId <= Integer.MAX_VALUE;
            return importer.add(0, builder, (int) (nodeId));
        }

        @Override
        void add(long nodeId, long targetId, SingleAdjacencyImporter importer) {
        }
    }

    private static final class NoAdjacency extends AdjacencyBuilder {

        private static final AdjacencyBuilder INSTANCE = new NoAdjacency();

        @Override
        void addAdjacencyImporter(
                AllocationTracker tracker,
                boolean loadDegrees,
                int pageIndex) {
        }

        @Override
        void finishPreparation() {
        }

        @Override
        int add(AdjacencyImporter importer, long nodeId) {
            return 0;
        }

        @Override
        void add(long nodeId, long targetId, SingleAdjacencyImporter importer) {
        }
    }
}
