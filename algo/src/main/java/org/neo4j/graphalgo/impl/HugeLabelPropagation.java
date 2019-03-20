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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeNodeProperties;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.LabelPropagationAlgorithm.HugeLabelArray;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.concurrent.ExecutorService;

public final class HugeLabelPropagation extends BaseLabelPropagation<
        HugeGraph,
        HugeWeightMapping,
        HugeLabelArray,
        HugeLabelPropagation> {

    public HugeLabelPropagation(
            HugeGraph graph,
            HugeNodeProperties nodeProperties,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker) {
        super(graph,
                nodeProperties.hugeNodeProperties(PARTITION_TYPE),
                nodeProperties.hugeNodeProperties(WEIGHT_TYPE),
                batchSize,
                concurrency,
                executor,
                tracker);
    }

    @Override
    HugeLabelArray initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new HugeLabelArray(HugeLongArray.newArray(nodeCount, tracker));
    }

    @Override
    List<BaseStep> baseSteps(
            final HugeGraph graph,
            final HugeLabelArray labels,
            final HugeWeightMapping nodeProperties,
            final HugeWeightMapping nodeWeights,
            final Direction direction,
            final boolean randomizeOrder) {
        return ParallelUtil.readParallel(
                concurrency,
                batchSize,
                graph,
                executor,
                (nodeOffset, nodeIds) -> {
                    InitStep initStep2 = new InitStep(
                            graph,
                            labels.labels,
                            direction,
                            randomizeOrder,
                            getProgressLogger(),
                            nodeIds,
                            nodeProperties,
                            nodeWeights
                    );
                    return asStep(initStep2);
                });
    }

    private static final class InitStep extends Initialization {

        private final HugeGraph graph;
        private final HugeLongArray existingLabels;
        private final Direction direction;
        private final boolean randomizeOrder;
        private final ProgressLogger progressLogger;
        private final PrimitiveLongIterable nodes;
        private final HugeWeightMapping nodeProperties;
        private final HugeWeightMapping nodeWeights;

        private InitStep(
                HugeGraph graph,
                HugeLongArray existingLabels,
                Direction direction,
                boolean randomizeOrder,
                ProgressLogger progressLogger,
                PrimitiveLongIterable nodes,
                HugeWeightMapping nodeProperties,
                HugeWeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.randomizeOrder = randomizeOrder;
            this.progressLogger = progressLogger;
            this.nodes = nodes;
            this.nodeProperties = nodeProperties;
            this.nodeWeights = nodeWeights;
        }

        @Override
        void setExistingLabels() {
            PrimitiveLongIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                long nodeId = iterator.next();
                long existingLabel = (long) nodeProperties.nodeWeight(nodeId, (double) nodeId);
                existingLabels.set(nodeId, existingLabel);
            }
        }

        @Override
        Computation computeStep() {
            return new ComputeStep(
                    graph,
                    existingLabels,
                    direction,
                    randomizeOrder,
                    progressLogger,
                    nodes,
                    nodeWeights
            );
        }
    }

    private static final class ComputeStep extends Computation implements HugeRelationshipConsumer {

        private final HugeGraph graph;
        private final HugeLongArray existingLabels;
        private final Direction direction;
        private final ProgressLogger progressLogger;
        private final PrimitiveLongIterable nodes;
        private final long maxNode;
        private final LongDoubleHashMap votes;
        private final HugeWeightMapping nodeWeights;

        private ComputeStep(
                HugeGraph graph,
                HugeLongArray existingLabels,
                Direction direction,
                boolean randomizeOrder,
                ProgressLogger progressLogger,
                PrimitiveLongIterable nodes,
                HugeWeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.progressLogger = progressLogger;
            this.nodes = RandomlySwitchingLongIterable.of(randomizeOrder, nodes);
            this.maxNode = graph.nodeCount() - 1L;
            this.votes = new LongDoubleScatterMap();
            this.nodeWeights = nodeWeights;
        }

        @Override
        boolean computeAll() {
            PrimitiveLongIterator iterator = nodes.iterator();
            boolean didChange = false;
            while (iterator.hasNext()) {
                didChange = compute(iterator.next(), didChange);
            }
            return didChange;
        }

        private boolean compute(long nodeId, boolean didChange) {
            votes.clear();
            long partition = existingLabels.get(nodeId);
            long previous = partition;
            graph.forEachRelationship(nodeId, direction, this);
            double weight = Double.NEGATIVE_INFINITY;
            for (LongDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    partition = vote.key;
                }
            }
            progressLogger.logProgress(nodeId, maxNode);
            if (partition != previous) {
                existingLabels.set(nodeId, partition);
                return true;
            }
            return didChange;
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId) {
            long partition = existingLabels.get(targetNodeId);
            double weight = graph.weightOf(sourceNodeId, targetNodeId) * nodeWeights.nodeWeight(targetNodeId);
            votes.addTo(partition, weight);
            return true;
        }

        @Override
        void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

            if (votes.keys != null) {
                votes.keys = EMPTY_LONGS;
                votes.clear();
                votes.keys = null;
                votes.values = null;
            }
        }
    }
}
