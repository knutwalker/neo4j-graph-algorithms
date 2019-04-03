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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.HugeLabelPropagation;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.PARTITION_TYPE;
import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.WEIGHT_TYPE;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LabelPropagationBenchmarkLdbc {

    @Param({"5"})
    int iterations;

    @Param({"10000"})
    int batchSize;

    private HeavyGraph heavy;
    private HugeGraph huge;
    private GraphDatabaseAPI db;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LabelPropagationProc.class);

        GraphLoader loader = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeProperties(
                        PropertyMapping.of(WEIGHT_TYPE, WEIGHT_TYPE, 1.0),
                        PropertyMapping.of(PARTITION_TYPE, PARTITION_TYPE, 0.0)
                )
                .withDirection(Direction.BOTH)
                .withExecutorService(Pools.DEFAULT);

        heavy = (HeavyGraph) loader.load(HeavyGraphFactory.class);
        huge = (HugeGraph) loader.load(HugeGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        heavy.release();
        huge.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public LabelPropagation heavy_graph_default_lpa() {
        return new org.neo4j.graphalgo.impl
                .LabelPropagation(heavy, heavy, batchSize, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute(Direction.OUTGOING, iterations);
    }

    @Benchmark
    public LabelPropagation huge_graph_default_lpa() {
        return new org.neo4j.graphalgo.impl
                .LabelPropagation(huge, huge, batchSize, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute(Direction.OUTGOING, iterations);
    }

    @Benchmark
    public HugeLabelPropagation huge_graph_huge_lpa() {
        return new org.neo4j.graphalgo.impl
                .HugeLabelPropagation(
                huge,
                huge,
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY)
                .compute(Direction.OUTGOING, iterations);
    }
}
