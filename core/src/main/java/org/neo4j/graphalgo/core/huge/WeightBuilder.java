package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class WeightBuilder {

    abstract void addWeightImporter(int pageIndex);

    abstract void finish();

    // TODO: check thread safety
    abstract void addWeight(
            CursorFactory cursors,
            Read read,
            long relationshipReference,
            long propertiesReference,
            long sourceNodeId,
            long targetNodeId);

    static WeightBuilder of(
            HugeWeightMapBuilder weights,
            int numPages,
            int pageSize,
            long nodeCount,
            AllocationTracker tracker) {
        if (!weights.loadsWeights()) {
            return NoWeights.INSTANCE;
        }
        weights.prepare(numPages, pageSize);

        tracker.add(sizeOfObjectArray(numPages));
        HugeWeightMapBuilder[] builders = new HugeWeightMapBuilder[numPages];
        return new PagedWeights(weights, builders, nodeCount, pageSize);
    }

    private static final class PagedWeights extends WeightBuilder {

        private final HugeWeightMapBuilder weights;
        private final HugeWeightMapBuilder[] builders;
        private final long nodeCount;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;

        private PagedWeights(
                HugeWeightMapBuilder weights,
                HugeWeightMapBuilder[] builders,
                long nodeCount,
                int pageSize) {
            this.weights = weights;
            this.builders = builders;
            this.nodeCount = nodeCount;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
        }

        void addWeightImporter(int pageIndex) {
            int pageSize = (int) Math.min((long) this.pageSize, nodeCount - (((long) pageIndex) << pageMask));
            if (pageSize > 0) {
                builders[pageIndex] = weights.threadLocalCopy(pageIndex, pageSize);
            }
        }

        @Override
        void finish() {
            int numBuilders = 0;
            for (; numBuilders < builders.length; numBuilders++) {
                HugeWeightMapBuilder builder = builders[numBuilders];
                if (builder == null) {
                    break;
                }
            }
            weights.finish(numBuilders);
        }

        @Override
        void addWeight(
                final CursorFactory cursors,
                final Read read,
                final long relationshipReference,
                final long propertiesReference,
                final long sourceNodeId,
                final long targetNodeId) {
            int pageIdx = (int) (sourceNodeId >>> pageShift);
            int localId = (int) (sourceNodeId & pageMask);
            builders[pageIdx].load(
                    relationshipReference,
                    propertiesReference,
                    targetNodeId,
                    localId,
                    cursors,
                    read
            );
        }
    }

    private static final class NoWeights extends WeightBuilder {

        private static final WeightBuilder INSTANCE = new NoWeights();

        @Override
        void addWeightImporter(int pageIndex) {
        }

        @Override
        void finish() {
        }

        @Override
        void addWeight(
                final CursorFactory cursors,
                final Read read,
                final long relationshipReference,
                final long propertiesReference,
                final long sourceNodeId,
                final long targetNodeId) {
        }
    }
}
