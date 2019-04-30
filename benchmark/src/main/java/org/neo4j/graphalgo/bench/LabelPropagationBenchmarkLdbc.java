/*
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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.LabelPropagation;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.PARTITION_TYPE;
import static org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.WEIGHT_TYPE;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 4, time = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LabelPropagationBenchmarkLdbc {

    @Param({"1", "15"})
    int iterations;

    private GraphDatabaseAPI db;
    private HeavyGraph graph;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb();
        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeProperties(
                        PropertyMapping.of(WEIGHT_TYPE, WEIGHT_TYPE, 1.0),
                        PropertyMapping.of(PARTITION_TYPE, PARTITION_TYPE, 0.0)
                )
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public LabelPropagation lpa() {
        return new org.neo4j.graphalgo.impl
                .LabelPropagation(graph, graph, ParallelUtil.DEFAULT_BATCH_SIZE, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute(Direction.OUTGOING, iterations);
    }
}
