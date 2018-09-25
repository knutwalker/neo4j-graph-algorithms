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

import org.jctools.queues.MpmcArrayQueue;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Queue;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class ScanningRelationshipImporter2 {

    static Runnable create(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            boolean loadDegrees,
            int concurrency) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            return new SerialScanning2(
                    setup, api, progress, tracker, idMap, weights,
                    outAdjacency, inAdjacency, loadDegrees);
        }
        return new ParallelScanning2(
                setup, api, dimensions, progress, tracker, idMap, weights, loadDegrees,
                outAdjacency, inAdjacency, threadPool, concurrency);
    }

    private ScanningRelationshipImporter2() {}
}

final class ParallelScanning2 implements Runnable {

    private static final int PER_THREAD_IN_FLIGHT = 1 << 7;

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final GraphDimensions dimensions;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final boolean loadDegrees;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    ParallelScanning2(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            boolean loadDegrees,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        this.setup = setup;
        this.api = api;
        this.dimensions = dimensions;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.loadDegrees = loadDegrees;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    @Override
    public void run() {
        long nodeCount = idMap.nodeCount();
        final ImportSizing sizing = ImportSizing.of(concurrency, nodeCount);

//        int numberOfImporterThreads = sizing.numberOfThreads() - 1;
        int numberOfImporterThreads = sizing.numberOfImporterThreads();
//        int numberOfImporterThreads = 1;
        int numberOfScannerThreads = Math.max(1, sizing.numberOfThreads() - numberOfImporterThreads);

        Queue<RelationshipsBatch> queue = new MpmcArrayQueue<>(1024);

        ImportingThreadPool.CreateImporter blueprint = createImporter(
                nodeCount,
                sizing.pageSize(),
                sizing.numberOfPages(),
                queue);

        ImportingThreadPool.CreateScanner creator = createScanner(
                sizing.pageSize(),
                sizing.numberOfPages(),
                numberOfImporterThreads,
                queue);

        ImportingThreadPool pool = new ImportingThreadPool(
                numberOfScannerThreads,
                creator,
                numberOfImporterThreads,
                blueprint);

        long tookNanos = pool.run(threadPool);

        DependencyResolver dep = api.getDependencyResolver();
        RelationshipStore relationshipStore = dep
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores()
                .getRelationshipStore();

        long relsInUse = 1L + relationshipStore.getHighestPossibleIdInUse();
        long recordsPerPage = (long) relationshipStore.getRecordsPerPage();
        long idsInPages = ((relsInUse + (recordsPerPage - 1L)) / recordsPerPage) * recordsPerPage;
        long requiredBytes = idsInPages * (long) relationshipStore.getRecordSize();

        double tookInSeconds = (double) tookNanos / 1_000_000_000.0;
        long bytesPerSecond = 1_000_000_000L * requiredBytes / tookNanos;
        long perThreadThroughput = bytesPerSecond / numberOfScannerThreads;

        setup.log.info(
                "Relstore size %d bytes (%s), import took %.3f s, overall %s/s (%d bytes/s) or per (%d) thread: %s/s (%d bytes/s)",
                requiredBytes,
                AllocationTracker.humanReadable(requiredBytes),
                tookInSeconds,
                AllocationTracker.humanReadable(bytesPerSecond),
                bytesPerSecond,
                numberOfScannerThreads,
                AllocationTracker.humanReadable(perThreadThroughput),
                perThreadThroughput
        );
    }

    private ImportingThreadPool.CreateImporter createImporter(
            long nodeCount,
            int pageSize,
            int numberOfPages,
            final Queue<RelationshipsBatch> queue) {
        WeightBuilder weightBuilder = WeightBuilder.of(weights, numberOfPages, pageSize, nodeCount, tracker);
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(outAdjacency, numberOfPages, pageSize, tracker);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(inAdjacency, numberOfPages, pageSize, tracker);

        for (int idx = 0; idx < numberOfPages; idx++) {
            weightBuilder.addWeightImporter(idx);
            outBuilder.addAdjacencyImporter(tracker, loadDegrees, idx);
            inBuilder.addAdjacencyImporter(tracker, loadDegrees, idx);
        }

        weightBuilder.finish();
        outBuilder.finishPreparation();
        inBuilder.finishPreparation();

//        ImporterObjectPool pool = new ImporterObjectPool();
        return RelationshipImporter2.createBlueprint(
                api,
                setup,
                progress,
                tracker,
                weightBuilder,
                outBuilder,
                inBuilder,
//                pool,
                queue);
    }

    private ImportingThreadPool.CreateScanner createScanner(
            int pageSize,
            int numberOfPages,
            int numberOfImporterThreads,
            Queue<RelationshipsBatch> queue) {

        int inFlight = Math.max(1, numberOfImporterThreads) * PER_THREAD_IN_FLIGHT;
        int baseQueueBatchSize = Math.max(1 << 4, Math.min(1 << 12, setup.batchSize));
        int queueBatchSize = baseQueueBatchSize << 2;

        PageCacheScanner cacheScanner = new PageCacheScanner(api, dimensions.singleRelationshipTypeId());
        ScannerBuffer outBuffer = null, inBuffer = null;
        int required = RelationshipsScanner2.requiredBuffer(setup, weights.loadsWeights());
        if (RelationshipsScanner2.requiresOut(required)) {
            outBuffer = ScannerBuffer.of(numberOfPages, pageSize, queueBatchSize);
        }
        if (RelationshipsScanner2.requiresIn(required)) {
            inBuffer = ScannerBuffer.of(numberOfPages, pageSize, queueBatchSize);
        }

        ScannerImporterQueue importerQueue = new ScannerImporterQueue(
                inFlight,
                queue,
                outBuffer,
                inBuffer);

        return QueueingScanner2.of(
                api, progress, idMap, cacheScanner, outBuffer, inBuffer,
                importerQueue, queueBatchSize, setup, weights);
    }
}

final class SerialScanning2 implements Runnable {

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final int nodeCount;
    private final boolean loadDegrees;

    SerialScanning2(
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            boolean loadDegrees) {
        this.loadDegrees = loadDegrees;
        if (idMap.nodeCount() > Integer.MAX_VALUE) {
            failForTooMuchNodes(idMap.nodeCount(), null);
        }
        this.nodeCount = (int) idMap.nodeCount();
        this.setup = setup;
        this.api = api;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
    }

    @Override
    public void run() {
        weights.prepare(1, 0);
        long[][] outDegrees, inDegrees;
        // TODO: todo
        final RelationshipImporter importer;
        try {
            HugeWeightMapBuilder threadWeights = weights.threadLocalCopy(0, nodeCount);
            outDegrees = createDegreesBuffer(nodeCount, tracker, outAdjacency);
            inDegrees = createDegreesBuffer(nodeCount, tracker, inAdjacency);
            importer = new RelationshipImporter(
                    api, progress, tracker, threadWeights, null, 0, 0L, nodeCount,
                    HugeAdjacencyBuilder.threadLocal(outAdjacency, degrees(outDegrees), loadDegrees),
                    HugeAdjacencyBuilder.threadLocal(inAdjacency, degrees(inDegrees), loadDegrees));
        } catch (OutOfMemoryError oom) {
            failForTooMuchNodes(idMap.nodeCount(), oom);
            return;
        }

        int baseQueueBatchSize = Math.max(1 << 4, Math.min(1 << 12, setup.batchSize));
        int queueBatchSize = 3 * baseQueueBatchSize;

        final RelationshipsScanner scanner = new NonQueueingScanner(
                api, progress, idMap, outDegrees, inDegrees, loadDegrees,
                queueBatchSize, setup, weights.loadsWeights(), importer);
        scanner.run();
    }

    private static long[][] createDegreesBuffer(
            final int nodeCount,
            final AllocationTracker tracker,
            final HugeAdjacencyBuilder adjacency) {
        if (adjacency != null) {
            long[][] degrees = new long[1][nodeCount];
            tracker.add(sizeOfLongArray(nodeCount) + sizeOfObjectArray(1));
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, 0));
            return degrees;
        }
        return null;
    }

    private static long[] degrees(long[][] degrees) {
        return degrees != null ? degrees[0] : null;
    }

    private static void failForTooMuchNodes(long nodeCount, Throwable cause) {
        String msg = String.format(
                "Cannot import %d nodes in a single thread, you have to provide a valid thread pool",
                nodeCount
        );
        throw new IllegalArgumentException(msg, cause);
    }
}
