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

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Queue;


final class RelationshipImporter2 extends StatementFunction<ImportingThreadPool.ComputationResult> {

    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final WeightBuilder weights;
    private final AdjacencyBuilder outAdjacency;
    private final AdjacencyBuilder inAdjacency;
    private final Queue<RelationshipsBatch> queue;
    private final ImportingThreadPool.CanStop canStop;
    private final int threadId;

    private Read read;
    private CursorFactory cursors;
    private boolean queueDone;

    private RelationshipImporter2(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            AllocationTracker tracker,
            WeightBuilder weights,
            AdjacencyBuilder outAdjacency,
            AdjacencyBuilder inAdjacency,
            Queue<RelationshipsBatch> queue,
            ImportingThreadPool.CanStop canStop,
            int threadId) {
        super(api);
        this.setup = setup;
        this.progress = progress;
        this.tracker = tracker;
        this.weights = weights;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
        this.queue = queue;
        this.canStop = canStop;
        this.threadId = threadId;
    }

    static ImportingThreadPool.CreateImporter createBlueprint(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            AllocationTracker tracker,
            WeightBuilder weights,
            AdjacencyBuilder outAdjacency,
            AdjacencyBuilder inAdjacency,
            Queue<RelationshipsBatch> queue) {
        return (id, canStop) -> new RelationshipImporter2(
                api,
                setup,
                progress,
                tracker,
                weights,
                outAdjacency,
                inAdjacency,
                queue,
                canStop,
                id);
    }

    @Override
    public ImportingThreadPool.ComputationResult call() {
        if (!needsToRun()) {
            return ImportingThreadPool.ComputationResult.DONE;
        }
        return super.call();
    }

    private boolean needsToRun() {
        return setup.loadAsUndirected || setup.loadOutgoing || setup.loadIncoming;
    }

    @Override
    public ImportingThreadPool.ComputationResult apply(final KernelTransaction transaction) {
        try {
            useKernelTransaction(transaction);
            return runImport();
        } catch (Exception e) {
            return drainQueue(e);
        } finally {
            unsetKernelTransaction();
        }
    }

    private ImportingThreadPool.ComputationResult runImport() {
        try {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        queueDone = true;
                        return ImportingThreadPool.ComputationResult.DONE;
                    }
                    if (relationship == null) {
                        if (canStop.canStop()) {
                            return ImportingThreadPool.ComputationResult.OUT_OF_WORK;
                        }
                    } else {
                        addRelationshipsBatch(relationship);
                    }
                }
            }
        } catch (Exception e) {
            Exceptions.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    void useKernelTransaction(KernelTransaction transaction) {
        read = transaction.dataRead();
        cursors = transaction.cursors();
    }

    private void unsetKernelTransaction() {
        read = null;
        cursors = null;
    }

    void pushBatch(RelationshipsBatch relationship) {
        if (relationship == RelationshipsBatch.SENTINEL) {
            queueDone = true;
            return;
        }
        try (RelationshipsBatch batch = relationship) {
            addRelationshipsBatch(batch);
        }
    }

    private ImportingThreadPool.ComputationResult drainQueue(Exception e) {
        if (!queueDone) {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        return ImportingThreadPool.ComputationResult.DONE;
                    }
                }
            }
        }
        Exceptions.throwIfUnchecked(e);
        throw new RuntimeException(e);
    }

    @Override
    public String threadName() {
        return "HugeRelationshipImport-" + threadId;
    }

    private RelationshipsBatch pollNext() {
        long retries = -1L;
        RelationshipsBatch batch;
        do {
            ++retries;
            batch = queue.poll();
        } while (batch == null && retries < 20_000L);
        return batch;
    }

    private void addRelationshipsBatch(RelationshipsBatch batch) {
        switch (batch.dataFlag()) {
            case RelationshipsBatch.JUST_RELATIONSHIPS:
                addRelationship(batch);
                break;
            case RelationshipsBatch.JUST_WEIGHTS:
                addWeight(batch);
                break;
            case RelationshipsBatch.RELS_AND_WEIGHTS:
                addRelationshipAndWeight(batch);
                break;
            default:
                throw new IllegalArgumentException("unsupported");
        }
    }

    private void addRelationshipAndWeight(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        final AdjacencyBuilder adjacency = batch.isOut() ? outAdjacency : inAdjacency;
        long source, target, relId, propId;
        for (int i = 0; i < length; i += 4) {
            source = sourceTargetIds[i];
            target = sourceTargetIds[1 + i];
            relId = sourceTargetIds[2 + i];
            propId = sourceTargetIds[3 + i];
            addRelationshipAndWeight(source, target, relId, propId, adjacency);
        }
        progress.relationshipsImported(batch.length >> 2);
    }

    private void addRelationshipAndWeight(long source, long target, long relId, long propId, AdjacencyBuilder builder) {
        addRelationship(source, target, builder);
        addWeight(source, target, relId, propId);
    }

    private void addRelationship(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        final AdjacencyBuilder adjacency = batch.isOut() ? outAdjacency : inAdjacency;
        long source, target;
        for (int i = 0; i < length; i += 2) {
            source = sourceTargetIds[i];
            target = sourceTargetIds[1 + i];
            addRelationship(source, target, adjacency);
        }
        progress.relationshipsImported(batch.length >> 1);
    }

    private void addRelationship(long source, long target, AdjacencyBuilder builder) {
        builder.add(source, target, this::addTarget);
    }

    private void addTarget(
            int localId,
            long targetId,
            int degree,
            HugeAdjacencyBuilder adjacency,
            CompressedLongArray[] targets,
            LongsRef buffer) {
        int currentDegree = 1;
        CompressedLongArray target = targets[localId];
        if (target != null) {
            currentDegree = target.add(targetId);
        } else {
            target = new CompressedLongArray(tracker, targetId, degree);
            targets[localId] = target;
        }
        if (currentDegree >= degree) {
            adjacency.applyVariableDeltaEncoding(target, buffer, localId);
            targets[localId] = null;
        }
    }

//    private void addTarget2(
//            int localId,
//            long targetId,
//            int degree, CompressedLongArray[] targets,
//            HugeAdjacencyBuilder adjacency) {
//        long offset = UnsafeArrayCas.getCheckedRefOffset(targets, localId);
//        int currentDegree;
//        CompressedLongArray targetToReturn = null, target;
//        try {
//            target = UnsafeArrayCas.uncheckedForceAcquireObject(targets, offset);
//            if (target == null) {
//                target = new CompressedLongArray(tracker, targetId, degree);
//                currentDegree = 1;
//            } else {
//                currentDegree = target.add(targetId);
//            }
//            if (currentDegree < degree) {
//                targetToReturn = target;
//            } else {
//                targetToReturn = null;
//            }
//        } finally {
//            UnsafeArrayCas.uncheckedReturnObject(targets, offset, targetToReturn);
//        }
//        if (currentDegree >= degree) {
//            final LongsRef buffer = pool.getBuffer();
//            try {
//                adjacency.applyVariableDeltaEncoding(target, buffer, localId);
//            } finally {
//                pool.returnBuffer(buffer);
//            }
//        }
//    }

    private void addWeight(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        long source, target, relId, propId;
        for (int i = 0; i < length; i += 4) {
            source = sourceTargetIds[i];
            target = sourceTargetIds[1 + i];
            relId = sourceTargetIds[2 + i];
            propId = sourceTargetIds[3 + i];
            addWeight(source, target, relId, propId);
        }
    }

    private void addWeight(long source, long target, long relId, long propId) {
        weights.addWeight(cursors, read, relId, propId, source, target);
    }

    @Override
    public String toString() {
        return "PerThreadBuilder{" + threadId + "}";
    }
}
