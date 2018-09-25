/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RenamesCurrentThread;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.core.huge.RelationshipsBatch.JUST_RELATIONSHIPS;
import static org.neo4j.graphalgo.core.huge.RelationshipsBatch.JUST_WEIGHTS;
import static org.neo4j.graphalgo.core.huge.RelationshipsBatch.RELS_AND_WEIGHTS;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;


abstract class RelationshipsScanner2 extends StatementFunction<ImportingThreadPool.ComputationResult> {

    private static final int REQUIRES_NONE = 0;
    private static final int REQUIRES_OUT = 1;
    private static final int REQUIRES_IN = 2;

    private final ImportProgress progress;
    private final HugeIdMap idMap;
    private final PageCacheScanner scanner;
    private final ScannerBuffer outBuffer;
    private final ScannerBuffer inBuffer;
    private final ScannerImporterQueue queue;
    private final int batchSize;
    private final Emit3 emit3;
    private final ImportingThreadPool.CanStop canStop;
    private final int scannerIndex;

    RelationshipsScanner2(
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            PageCacheScanner scanner,
            ScannerBuffer outBuffer,
            ScannerBuffer inBuffer,
            ScannerImporterQueue queue,
            int batchSize,
            GraphSetup setup,
            boolean loadWeights,
            ImportingThreadPool.CanStop canStop,
            int threadIndex) {
        super(api);
        assert (batchSize % 4) == 0 : "batchSize must be divisible by three";
        this.progress = progress;
        this.idMap = idMap;
        this.scanner = scanner;
        this.outBuffer = outBuffer;
        this.inBuffer = inBuffer;
        this.queue = queue;
        this.batchSize = batchSize;
        this.emit3 = setupEmitter3(setup, loadWeights);
//        this.emit3 = this::emitNothing;
        this.canStop = canStop;
        this.scannerIndex = threadIndex;
    }

//    private Emit2 setupEmitter(
//            GraphSetup setup,
//            boolean loadWeights,
//            int batchSize,
//            int threadSize) {
//        if (setup.loadAsUndirected) {
//            if (loadWeights) {
//                return new EmitUndirected2(threadSize, batchSize);
//            }
//            return new EmitUndirectedNoWeight2(threadSize, batchSize);
//        }
//        if (setup.loadOutgoing && setup.loadIncoming) {
//            if (loadWeights) {
//                return new EmitBoth2(threadSize, batchSize);
//            }
//            return new EmitBothNoWeight2(threadSize, batchSize);
//        }
//        if (setup.loadOutgoing) {
//            if (loadWeights) {
//                return new EmitOut2(threadSize, batchSize);
//            }
//            return new EmitOutNoWeight2(threadSize, batchSize);
//        }
//        if (setup.loadIncoming) {
//            if (loadWeights) {
//                return new EmitIn2(threadSize, batchSize);
//            }
//            return new EmitInNoWeight2(threadSize, batchSize);
//        }
//        return new EmitNone2();
//    }

    private Emit3 setupEmitter3(GraphSetup setup, boolean loadWeights) {
        if (loadWeights) {
            if (setup.loadAsUndirected) {
                return this::emitUndirectedWithWeight;
            }
            if (setup.loadOutgoing) {
                return setup.loadIncoming
                        ? this::emitBothWithWeight
                        : this::emitOutWithWeight;
            }
            if (setup.loadIncoming) {
                return this::emitInWithWeight;
            }
        } else {
            if (setup.loadAsUndirected) {
                return this::emitUndirectedNoWeight;
            }
            if (setup.loadOutgoing) {
                return setup.loadIncoming
                        ? this::emitBothNoWeight
                        : this::emitOutNoWeight;
            }
            if (setup.loadIncoming) {
                return this::emitInNoWeight;
            }
        }

        return this::emitNothing;
    }

    static int requiredBuffer(GraphSetup setup, boolean loadWeights) {
        if (setup.loadAsUndirected) {
            return REQUIRES_OUT;
        }
        int requires = REQUIRES_NONE;

        if (setup.loadOutgoing) {
            requires |= REQUIRES_OUT;
        }
        if (setup.loadIncoming) {
            requires |= REQUIRES_IN;
            if (loadWeights) {
                requires |= REQUIRES_OUT;
            }
        }

        return requires;
    }

    static boolean requiresOut(int required) {
        return (required & REQUIRES_OUT) == REQUIRES_OUT;
    }

    static boolean requiresIn(int required) {
        return (required & REQUIRES_IN) == REQUIRES_IN;
    }

    static void drainBuffers(GraphSetup setup, boolean loadWeights, ScannerImporterQueue queue) {
        if (loadWeights) {
            if (setup.loadAsUndirected) {
                queue.drainOutBuffer(OUTGOING, RELS_AND_WEIGHTS);
            }
            if (setup.loadOutgoing) {
                if (setup.loadIncoming) {
                    queue.drainBuffer(OUTGOING, RELS_AND_WEIGHTS, INCOMING, JUST_RELATIONSHIPS);
                } else {
                    queue.drainOutBuffer(OUTGOING, RELS_AND_WEIGHTS);
                }
            }
            if (setup.loadIncoming) {
                queue.drainBuffer(INCOMING, JUST_WEIGHTS, INCOMING, JUST_RELATIONSHIPS);
            }
        } else {
            if (setup.loadAsUndirected) {
                queue.drainOutBuffer(OUTGOING, JUST_RELATIONSHIPS);
            }
            if (setup.loadOutgoing) {
                if (setup.loadIncoming) {
                    queue.drainBuffer(OUTGOING, JUST_RELATIONSHIPS, INCOMING, JUST_RELATIONSHIPS);
                } else {
                    queue.drainOutBuffer(OUTGOING, JUST_RELATIONSHIPS);
                }
            }
            if (setup.loadIncoming) {
                queue.drainInBuffer(INCOMING, JUST_RELATIONSHIPS);
            }
        }

    }


    @Override
    public final String threadName() {
        return "relationship-store-scan";
    }

    @Override
    public ImportingThreadPool.ComputationResult apply(final KernelTransaction transaction) {
        prepareTransaction(transaction);
        return scanRelationships();
    }

    abstract void prepareTransaction(KernelTransaction transaction);

//    abstract void sendBatchToImportThread(RelationshipsBatch rel);
//
//    abstract void sendSentinelToImportThread();

    private ImportingThreadPool.ComputationResult scanRelationships() {
        try (Revert ignore = RenamesCurrentThread.renameThread("huge-scan-relationships-" + scannerIndex);
             PageCacheScanner.Cursor cursor = scanner.getCursor()) {
            final HugeIdMap idMap = this.idMap;
            final ImportProgress progress = this.progress;
            long source, target;
            int imported = 0;
            boolean done = true;

            while (cursor.next()) {
                source = idMap.toHugeMappedNodeId(cursor.sourceNodeReference());
                if (source != -1L) {
                    target = idMap.toHugeMappedNodeId(cursor.targetNodeReference());
                    if (target != -1L) {
                        long retries = emit3.emit(source, target, cursor);
                        if (retries > 0L && canStop.canStop()) {
                            done = false;
                            break;
                        }
                    }
                }
                if (++imported == 100_000) {
                    progress.relationshipsImported(100_000);
                    imported = 0;
                }
            }
            progress.relationshipsImported(imported);
            return done
                    ? ImportingThreadPool.ComputationResult.DONE
                    : ImportingThreadPool.ComputationResult.OUT_OF_WORK;
        }
    }

    private long emitUndirectedWithWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationshipWithProperties(
                source,
                target,
                relData.relationshipReference(),
                relData.propertiesReference(),
                queue,
                RELS_AND_WEIGHTS,
                OUTGOING) +
                outBuffer.addRelationshipWithProperties(
                        target,
                        source,
                        relData.relationshipReference(),
                        relData.propertiesReference(),
                        queue,
                        RELS_AND_WEIGHTS,
                        OUTGOING);
    }

    private long emitUndirectedNoWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationship(source, target, queue, JUST_RELATIONSHIPS, OUTGOING) +
                outBuffer.addRelationship(target, source, queue, JUST_RELATIONSHIPS, OUTGOING);
    }

    private long emitBothWithWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationshipWithProperties(
                source,
                target,
                relData.relationshipReference(),
                relData.propertiesReference(),
                queue,
                RELS_AND_WEIGHTS,
                OUTGOING) +
                inBuffer.addRelationship(target, source, queue, JUST_RELATIONSHIPS, INCOMING);
    }

    private long emitBothNoWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationship(source, target, queue, JUST_RELATIONSHIPS, OUTGOING) +
                inBuffer.addRelationship(target, source, queue, JUST_RELATIONSHIPS, INCOMING);
    }

    private long emitOutWithWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationshipWithProperties(
                source,
                target,
                relData.relationshipReference(),
                relData.propertiesReference(),
                queue,
                RELS_AND_WEIGHTS,
                OUTGOING);
    }

    private long emitOutNoWeight(long source, long target, RelationshipDataAccessor relData) {
        return outBuffer.addRelationship(source, target, queue, JUST_RELATIONSHIPS, OUTGOING);
    }

    private long emitInWithWeight(long source, long target, RelationshipDataAccessor relData) {
        return inBuffer.addRelationship(target, source, queue, JUST_RELATIONSHIPS, INCOMING) +
                outBuffer.addRelationshipWithProperties(
                        source,
                        target,
                        relData.relationshipReference(),
                        relData.propertiesReference(),
                        queue,
                        JUST_WEIGHTS,
                        INCOMING);
    }

    private long emitInNoWeight(long source, long target, RelationshipDataAccessor relData) {
        return inBuffer.addRelationship(target, source, queue, JUST_RELATIONSHIPS, INCOMING);
    }

    private long emitNothing(long source, long target, RelationshipDataAccessor relData) {
        return 0L;
    }

//    private void batchRelationship(long source, long target, LongsBuffer buffer, Direction direction) {
//        int threadIndex = threadIndex(source);
//        int len = buffer.addRelationship(threadIndex, source, target);
//        if (len >= batchSize) {
//            sendRelationship(threadIndex, len, buffer, direction);
//        }
//    }
//
//    private void batchRelationshipWithId(
//            long source,
//            long target,
//            RelationshipDataAccessor relData,
//            LongsBuffer buffer,
//            Direction direction) {
//        int threadIndex = threadIndex(source);
//        int len = buffer.addRelationshipWithId(
//                threadIndex,
//                source,
//                target,
//                relData.relationshipReference(),
//                relData.propertiesReference());
//        if (len >= batchSize) {
//            sendRelationship(threadIndex, len, buffer, direction);
//        }
//    }

//    private void batchRelationship(
//            ScannerBuffer buffer,
//            long source,
//            long target,
//            int baseFlags,
//            Direction direction) {
//        buffer.addRelationship(source, target, queue, baseFlags, direction);
//    }
//
//    private void batchRelationshipWithProperties(
//            ScannerBuffer buffer,
//            long source,
//            long target,
//            RelationshipDataAccessor relData,
//            int baseFlags,
//            Direction direction) {
//        buffer.addRelationshipWithProperties(
//                source,
//                target,
//                relData.relationshipReference(),
//                relData.propertiesReference(),
//                queue,
//                baseFlags,
//                direction);
//    }

//    private void sendRelationship(
//            int threadIndex,
//            int length,
//            LongsBuffer buffer,
//            Direction direction) {
//        if (length == 0) {
//            return;
//        }
//        RelationshipsBatch batch = nextRelationshipBatch(buffer.baseFlags, direction);
//        long[] newBuffer = setRelationshipBatch(batch, buffer.get(threadIndex), length);
//        buffer.reset(threadIndex, newBuffer);
//        sendBatchToImportThread(batch);
//    }

//    private void sendRelationshipOut(int baseFlags, long[] targets, int length) {
//        sendRelationship(baseFlags, targets, length, Direction.OUTGOING);
//    }
//
//    private void sendRelationshipIn(int baseFlags, long[] targets, int length) {
//        sendRelationship(baseFlags, targets, length, Direction.INCOMING);
//    }

//    private void sendRelationship(
//            int baseFlags,
//            long[] targets,
//            int length,
//            Direction direction) {
//        if (length == 0) {
//            return;
//        }
//        RelationshipsBatch batch = nextRelationshipBatch(baseFlags, direction);
//        setRelationshipBatch(batch, targets, length);
//        sendBatchToImportThread(batch);
//    }

//    private RelationshipsBatch nextRelationshipBatch(
//            final int baseFlags,
//            final Direction direction) {
//        RelationshipsBatch loaded;
//        do {
//            loaded = pool.poll();
//        } while (loaded == null);
//        loaded.setInfo(direction, baseFlags);
//        return loaded;
//    }
//
//    private long[] setRelationshipBatch(RelationshipsBatch loaded, long[] batch, int length) {
//        loaded.length = length;
//        if (loaded.sourceTargetIds == null) {
//            loaded.sourceTargetIds = batch;
//            return new long[length];
//        }
//        long[] sourceAndTargets = loaded.sourceTargetIds;
//        loaded.sourceTargetIds = batch;
//        return sourceAndTargets;
//    }

//    private void sendLastBatch() {
//        emit.emitLastBatch(this);
//    }

//    private void sendSentinels() {
//        for (int i = 0; i < numberOfThreads; i++) {
//            sendSentinelToImportThread();
//        }
//    }

//    private void releaseBufferPool() {
//        if (pool != null) {
//            AbstractCollection<RelationshipsBatch> consumer = new AbstractCollection<RelationshipsBatch>() {
//                @Override
//                public Iterator<RelationshipsBatch> iterator() {
//                    return Collections.emptyIterator();
//                }
//
//                @Override
//                public int size() {
//                    return 0;
//                }
//
//                @Override
//                public boolean add(final RelationshipsBatch relationshipsBatch) {
//                    relationshipsBatch.sourceTargetIds = null;
//                    relationshipsBatch.length = 0;
//                    return false;
//                }
//            };
//            pool.drainTo(consumer);
//        }
//    }

//    private static ArrayBlockingQueue<RelationshipsBatch> newPool(int capacity) {
//        final ArrayBlockingQueue<RelationshipsBatch> rels = new ArrayBlockingQueue<>(capacity);
//        int i = capacity;
//        while (i-- > 0) {
//            rels.add(new RelationshipsBatch(rels));
//        }
//        return rels;
//    }

    @FunctionalInterface
    private interface Emit3 {
        long emit(long source, long target, RelationshipDataAccessor relData);
    }
}

final class QueueingScanner2 extends RelationshipsScanner2 {

    static ImportingThreadPool.CreateScanner of(
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            PageCacheScanner scanner,
            ScannerBuffer outBuffer,
            ScannerBuffer inBuffer,
            ScannerImporterQueue queue,
            int batchSize,
            GraphSetup setup,
            HugeWeightMapBuilder weights) {
        return new ImportingThreadPool.CreateScanner() {

            @Override
            public void onScannerFinish(final int numberOfRunningImporters) {
                RelationshipsScanner2.drainBuffers(setup, weights.loadsWeights(), queue);
                queue.sendSentinels(numberOfRunningImporters);
                queue.drainBatchPool();
            }

            @Override
            public RelationshipsScanner2 create(int index, ImportingThreadPool.CanStop canStop) {
                return new QueueingScanner2(
                        api,
                        progress,
                        idMap,
                        scanner,
                        outBuffer,
                        inBuffer,
                        queue,
                        batchSize,
                        setup,
                        weights.loadsWeights(),
                        canStop,
                        index);
            }
        };
    }

    private QueueingScanner2(
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            PageCacheScanner scanner,
            ScannerBuffer outBuffer,
            ScannerBuffer inBuffer,
            ScannerImporterQueue queue,
            int batchSize,
            GraphSetup setup,
            boolean loadWeights,
            ImportingThreadPool.CanStop canStop,
            int threadIndex) {
        super(
                api,
                progress,
                idMap,
                scanner,
                outBuffer,
                inBuffer,
                queue,
                batchSize,
                setup,
                loadWeights,
                canStop,
                threadIndex);
    }

//    @Override
//    void sendBatchToImportThread(RelationshipsBatch rel) {
//        sendToImportThread(threadQueue, rel);
//    }
//
//    @Override
//    void sendSentinelToImportThread() {
//        if (forwardSentinel) {
//            sendToImportThread(threadQueue, RelationshipsBatch.SENTINEL);
//        }
//    }

//    static void sendSentinelToImportThreads(Queue<RelationshipsBatch> queue, int numberOfThreads) {
//        for (int i = 0; i < numberOfThreads; i++) {
//            sendToImportThread(queue, RelationshipsBatch.SENTINEL);
//        }
//    }

//    private static void sendToImportThread(Queue<RelationshipsBatch> queue, RelationshipsBatch batch) {
//        do {
//            if (queue.offer(batch)) {
//                return;
//            }
//        } while (true);
//    }

    @Override
    void prepareTransaction(final KernelTransaction transaction) {
    }
}

final class NonQueueingScanner2 extends RelationshipsScanner2 {

    private final RelationshipImporter2 builder;

    NonQueueingScanner2(
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            PageCacheScanner scanner,
            ScannerBuffer outBuffer,
            ScannerBuffer inBuffer,
            ScannerImporterQueue queue,
            int batchSize,
            GraphSetup setup,
            boolean loadWeights,
            RelationshipImporter2 builder,
            ImportingThreadPool.CanStop canStop) {
        super(
                api,
                progress,
                idMap,
                scanner,
                outBuffer,
                inBuffer,
                queue,
                batchSize,
                setup,
                loadWeights,
                canStop,
                0);
        this.builder = builder;
    }

//    @Override
//    void sendBatchToImportThread(RelationshipsBatch rel) {
//        builder.pushBatch(rel);
//    }
//
//    @Override
//    void sendSentinelToImportThread() {
//        builder.pushBatch(RelationshipsBatch.SENTINEL);
//    }

    @Override
    void prepareTransaction(final KernelTransaction transaction) {
        builder.useKernelTransaction(transaction);
    }
}
