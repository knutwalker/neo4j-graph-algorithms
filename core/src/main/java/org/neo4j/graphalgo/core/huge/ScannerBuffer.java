package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

abstract class ScannerBuffer {

    @FunctionalInterface
    interface DrainConsumer {
        void apply(long[] bucket, int bucketLength);
    }

    abstract long addRelationship(
            long source,
            long target,
            ScannerImporterQueue queue,
            int baseFlags,
            Direction direction);

    abstract long addRelationshipWithProperties(
            long source,
            long target,
            long relId,
            long propId,
            ScannerImporterQueue queue,
            int baseFlags,
            Direction direction);

//    abstract int swap(long nodeId, RelationshipsBatch batch);

    abstract void drainAndRelease(DrainConsumer consumer);

    static ScannerBuffer of(int numberOfPages, int pageSize, int batchSize) {
        return new PagedBuffer(numberOfPages, pageSize, batchSize);
    }

    private static final class PagedBuffer extends ScannerBuffer {

        private int pageShift;
        private int batchSize;
        private ReentrantLock[] locks;
        private long[][] targets;
        private int[] lengths;

        private PagedBuffer(int numberOfPages, int pageSize, int batchSize) {
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.batchSize = batchSize;
            if (numberOfPages > 0) {
                locks = new ReentrantLock[numberOfPages];
                targets = new long[numberOfPages][batchSize];
                lengths = new int[numberOfPages];
                Arrays.setAll(locks, __ -> new ReentrantLock());
            }
        }

//        @Override
//        void addRelationship(
//                long source,
//                long target,
//                ScannerImporterQueue queue,
//                int baseFlags,
//                Direction direction) {
//            int pageIndex = (int) (source >>> pageShift);
//            long offset = UnsafeArrayCas.getCheckedRefOffset(targets, pageIndex);
//            long lengthsOffset = UnsafeArrayCas.getCheckedIntOffset(lengths, pageIndex);
//            long[] sourceTargetIds = UnsafeArrayCas.uncheckedForceAcquireObject(targets, offset);
//            try {
//                int current = UnsafeArrayCas.uncheckedIncrement(lengths, lengthsOffset, 2);
//                if (current >= batchSize) {
//                    RelationshipsBatch batch = queue.nextRelationshipBatch(baseFlags, direction);
//                    sourceTargetIds = batch.setData(sourceTargetIds, current, batchSize);
//                    UnsafeArrayCas.uncheckedSet(lengths, lengthsOffset, 2);
//                    queue.forceSendBatch(batch);
//                    current = 0;
//                }
//                sourceTargetIds[current] = source;
//                sourceTargetIds[1 + current] = target;
//            } finally {
//                UnsafeArrayCas.uncheckedReturnObject(targets, offset, sourceTargetIds);
//            }
//        }

        @Override
        long addRelationship(
                long source,
                long target,
                ScannerImporterQueue queue,
                int baseFlags,
                Direction direction) {
            int pageIndex = (int) (source >>> pageShift);
            ReentrantLock lock = locks[pageIndex];
            lock.lock();
            try {
                long[] sourceTargetIds = targets[pageIndex];
                if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
                    throw new IllegalArgumentException(String.format(
                            "got unexpected tombstone for source = [%d], target = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
                            source,
                            target,
                            queue,
                            baseFlags,
                            direction));
                }
                int length = lengths[pageIndex] += 2;
                sourceTargetIds[length - 2] = source;
                sourceTargetIds[length - 1] = target;
                if (length >= batchSize) {
                    if (length != batchSize) {
                        System.out.println("length = " + length);
                    }
                    RelationshipsBatch batch = queue.nextRelationshipBatch(baseFlags, direction);
                    sourceTargetIds = batch.setData(sourceTargetIds, length, batchSize);
                    if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
                        throw new IllegalArgumentException(String.format(
                                "got unexpected tombstone for batch = [%d], target = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
                                source,
                                target,
                                queue,
                                baseFlags,
                                direction));
                    }
                    lengths[pageIndex] = 0;
                    return queue.forceSendBatch(batch);
                }
                return 0L;
            } finally {
                lock.unlock();
            }
        }

        @Override
        long addRelationshipWithProperties(
                long source,
                long target,
                long relId,
                long propId,
                ScannerImporterQueue queue,
                int baseFlags,
                Direction direction) {
            int pageIndex = (int) (source >>> pageShift);
            ReentrantLock lock = locks[pageIndex];
            lock.lock();
            try {
                long[] sourceTargetIds = targets[pageIndex];
                if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
                    throw new IllegalArgumentException(String.format(
                            "got unexpected tombstone for source = [%d], target = [%d], relId = [%d], propId = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
                            source,
                            target,
                            relId,
                            propId,
                            queue,
                            baseFlags,
                            direction));
                }
                int length = lengths[pageIndex] += 4;
                sourceTargetIds[length - 4] = source;
                sourceTargetIds[length - 3] = target;
                sourceTargetIds[length - 2] = relId;
                sourceTargetIds[length - 1] = propId;
                if (length >= batchSize) {
                    if (length != batchSize) {
                        System.out.println("length = " + length);
                    }
                    RelationshipsBatch batch = queue.nextRelationshipBatch(baseFlags, direction);
                    sourceTargetIds = batch.setData(sourceTargetIds, length, batchSize);
                    if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
                        throw new IllegalArgumentException(String.format(
                                "got unexpected tombstone for batch = [%d], target = [%d], relId = [%d], propId = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
                                source,
                                target,
                                relId,
                                propId,
                                queue,
                                baseFlags,
                                direction));
                    }
                    lengths[pageIndex] = 0;
                    return queue.forceSendBatch(batch);
                }
                return 0L;
            } finally {
                lock.unlock();
            }
        }

//        @Override
//        void addRelationshipWithProperties(
//                long source,
//                long target,
//                long relId,
//                long propId,
//                ScannerImporterQueue queue,
//                int baseFlags,
//                Direction direction) {
//            int pageIndex = (int) (source >>> pageShift);
//            long offset = UnsafeArrayCas.getCheckedRefOffset(targets, pageIndex);
//            long lengthsOffset = UnsafeArrayCas.getCheckedIntOffset(lengths, pageIndex);
//            long[] sourceTargetIds = UnsafeArrayCas.uncheckedForceAcquireObject(targets, offset);
//            try {
//                if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
//                    throw new IllegalArgumentException(String.format(
//                            "got unexpected tombstone for source = [%d], target = [%d], relId = [%d], propId = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
//                            source,
//                            target,
//                            relId,
//                            propId,
//                            queue,
//                            baseFlags,
//                            direction));
//                }
//                int current = UnsafeArrayCas.uncheckedIncrement(lengths, lengthsOffset, 4);
//                if (current >= batchSize) {
//                    if (current != batchSize) {
//                        System.out.println("length = " + current);
//                    }
//                    RelationshipsBatch batch = queue.nextRelationshipBatch(baseFlags, direction);
//                    sourceTargetIds = batch.setData(sourceTargetIds, current, batchSize);
//                    if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
//                        throw new IllegalArgumentException(String.format(
//                                "got unexpected tombstone for batch = [%d], target = [%d], relId = [%d], propId = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
//                                source,
//                                target,
//                                relId,
//                                propId,
//                                queue,
//                                baseFlags,
//                                direction));
//                    }
//                    UnsafeArrayCas.uncheckedSet(lengths, lengthsOffset, 4);
//                    queue.forceSendBatch(batch);
//                    current = 0;
//                }
//                sourceTargetIds[current] = source;
//                sourceTargetIds[1 + current] = target;
//                sourceTargetIds[2 + current] = relId;
//                sourceTargetIds[3 + current] = propId;
//            } finally {
//                if (UnsafeArrayCas.isTombstone(sourceTargetIds)) {
//                    throw new IllegalArgumentException(String.format(
//                            "got unexpected tombstone inside finally = [%d], target = [%d], relId = [%d], propId = [%d], queue = [%s], baseFlags = [%d], direction = [%s]",
//                            source,
//                            target,
//                            relId,
//                            propId,
//                            queue,
//                            baseFlags,
//                            direction));
//                }
//                UnsafeArrayCas.uncheckedReturnObject(targets, offset, sourceTargetIds);
//            }
//        }

//        @Override
//        int swap(long nodeId, RelationshipsBatch batch) {
//            int pageIndex = (int) (nodeId >>> pageShift);
//            long offset = UnsafeArrayCas.getCheckedRefOffset(targets, pageIndex);
//            long[] values = UnsafeArrayCas.uncheckedForceAcquireObject(targets, offset);
//            long[] nextValues = null;
//            try {
//                int length = UnsafeArrayCas.getAndSet(lengths, pageIndex, 0);
//                if (length != batchSize) {
//                    System.out.println("length = " + length);
//                }
//                nextValues = batch.setData(values, length, batchSize);
//                return length;
//            } finally {
//                UnsafeArrayCas.uncheckedReturnObject(targets, offset, nextValues);
//            }
//        }

        synchronized void drainAndRelease(DrainConsumer consumer) {
            if (targets != null) {
                long[][] targets = this.targets;
                this.targets = null;
                int[] lengths = this.lengths;
                this.lengths = null;
                int length = targets.length;
                for (int i = 0; i < length; i++) {
                    consumer.apply(targets[i], lengths[i]);
                    targets[i] = null;
                }
            }
        }
    }
}
