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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.HugeIdMap;
import org.neo4j.graphalgo.core.huge.loader.ImportingThreadPool.ImportResult;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.utils.paged.AllocationTracker.humanReadable;


abstract class ScanningRelationshipImporter implements Runnable {

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
    private static final BigInteger A_BILLION = BigInteger.valueOf(1_000_000_000L);

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
        return new ScanningRelationshipImporter(
                setup, api, dimensions, progress, tracker, idMap, weights, loadDegrees,
                outAdjacency, inAdjacency, threadPool, concurrency) {
        };
    }

    private ScanningRelationshipImporter(
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
        int numberOfThreads = sizing.numberOfThreads();

        RelationshipStoreScanner scanner = RelationshipStoreScanner.of(api);
        ImportingThreadPool.CreateScanner creator = createScanner(
                nodeCount,
                sizing.pageSize(),
                sizing.numberOfPages(), scanner);

        ImportingThreadPool pool = new ImportingThreadPool(numberOfThreads, creator);

        ImportResult importResult = pool.run(threadPool);

        long requiredBytes = scanner.storeSize();
        long relsImported = importResult.relsImported;
        BigInteger bigNanos = BigInteger.valueOf(importResult.tookInNanos);
        double tookInSeconds = new BigDecimal(bigNanos).divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING).doubleValue();
        long bytesPerSecond = A_BILLION.multiply(BigInteger.valueOf(requiredBytes)).divide(bigNanos).longValueExact();

        setup.log.info(
                "Relationship Store Scan: Imported %,d records from %s (%,d bytes); took %.3f s, %,.2f records/s, %s/s (%,d bytes/s) (per thread: %,.2f records/s, %s/s (%,d bytes/s))",
                relsImported,
                humanReadable(requiredBytes),
                requiredBytes,
                tookInSeconds,
                (double) relsImported / tookInSeconds,
                humanReadable(bytesPerSecond),
                bytesPerSecond,
                (double) relsImported / tookInSeconds / numberOfThreads,
                humanReadable(bytesPerSecond / numberOfThreads),
                bytesPerSecond / numberOfThreads
        );
    }

    private ImportingThreadPool.CreateScanner createScanner(
            long nodeCount,
            int pageSize,
            int numberOfPages,
            RelationshipStoreScanner scanner) {
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

        return RelationshipsScanner.of(
                api, setup, progress, idMap, scanner, dimensions.singleRelationshipTypeId(),
                tracker, weightBuilder, outBuilder, inBuilder);
    }
}
