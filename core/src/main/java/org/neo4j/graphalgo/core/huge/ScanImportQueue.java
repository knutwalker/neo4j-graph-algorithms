package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphdb.Direction;

import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

abstract class ScanImportQueue {

    abstract RelationshipsBatch nextRelationshipBatch(int baseFlags, Direction direction);

    abstract void releaseRelationshipBatchPool();

    abstract int threadIndex(long nodeId);

    abstract int threadLocalId(long nodeId);

    abstract void sendBatchToImportThread(int threadIndex, RelationshipsBatch rel) throws InterruptedException;

    abstract void sendSentinelToImportThread(int threadIndex) throws InterruptedException;


    private static final class ParallelScanner extends ScanImportQueue {

        private final BlockingQueue<RelationshipsBatch>[] threadQueues;
        private final ArrayBlockingQueue<RelationshipsBatch> pool;
        private final int threadsShift;
        private final long threadsMask;
        private final boolean forwardSentinel;

        ParallelScanner(
                boolean forwardSentinel,
                BlockingQueue<RelationshipsBatch>[] threadQueues,
                int perThreadSize) {
            assert BitUtil.isPowerOfTwo(perThreadSize);
            this.forwardSentinel = forwardSentinel;
            this.threadQueues = threadQueues;
            this.threadsShift = Integer.numberOfTrailingZeros(perThreadSize);
            this.threadsMask = (long) (perThreadSize - 1);
            pool = newPool(10);
        }

        @Override
        RelationshipsBatch nextRelationshipBatch(final int baseFlags, final Direction direction) {
//            RelationshipsBatch loaded = pool.take();
//            loaded.setInfo(direction, baseFlags);
//            return loaded;
            return null;
        }

        @Override
        void releaseRelationshipBatchPool() {
            if (pool != null) {
                AbstractCollection<RelationshipsBatch> consumer = new AbstractCollection<RelationshipsBatch>() {
                    @Override
                    public Iterator<RelationshipsBatch> iterator() {
                        return Collections.emptyIterator();
                    }

                    @Override
                    public int size() {
                        return 0;
                    }

                    @Override
                    public boolean add(final RelationshipsBatch relationshipsBatch) {
                        relationshipsBatch.sourceTargetIds = null;
                        relationshipsBatch.length = 0;
                        return false;
                    }
                };
                pool.drainTo(consumer);
            }
        }

        @Override
        int threadIndex(long nodeId) {
            return (int) (nodeId >>> threadsShift);
        }

        @Override
        int threadLocalId(long nodeId) {
            return (int) (nodeId & threadsMask);
        }

        @Override
        void sendBatchToImportThread(int threadIndex, RelationshipsBatch rel) throws InterruptedException {
            threadQueues[threadIndex].put(rel);
        }

        @Override
        void sendSentinelToImportThread(int threadIndex) throws InterruptedException {
            if (forwardSentinel) {
                threadQueues[threadIndex].put(RelationshipsBatch.SENTINEL);
            }
        }

        private static ArrayBlockingQueue<RelationshipsBatch> newPool(int capacity) {
            final ArrayBlockingQueue<RelationshipsBatch> rels = new ArrayBlockingQueue<>(capacity);
            int i = capacity;
            while (i-- > 0) {
                rels.add(new RelationshipsBatch(rels));
            }
            return rels;
        }

    }
}
