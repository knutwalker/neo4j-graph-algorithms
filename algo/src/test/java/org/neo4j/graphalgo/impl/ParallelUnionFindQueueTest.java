package org.neo4j.graphalgo.impl;

import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.HugeWeightedRelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.IntStream;

import static org.junit.Assert.assertSame;

public final class ParallelUnionFindQueueTest {

    @Test(timeout = 2000L)
    public void testQueueSafetyUnderFailure() {
        IllegalStateException error = new IllegalStateException("some error");
        FlakyGraph graph = new FlakyGraph(100, 10, new Random(42L), error);
        ParallelUnionFindQueue uf = new ParallelUnionFindQueue(
                graph,
                Pools.DEFAULT,
                10,
                10
        );
        try {
            uf.compute();
        } catch (IllegalStateException e) {
            assertSame(error, e);
        }
    }

    @Test(timeout = 2000L)
    public void testHugeQueueSafetyUnderFailure() {
        IllegalStateException error = new IllegalStateException("some error");
        FlakyGraph graph = new FlakyGraph(100, 10, new Random(42L), error);
        HugeParallelUnionFindQueue uf = new HugeParallelUnionFindQueue(
                graph,
                Pools.DEFAULT,
                10,
                10,
                AllocationTracker.EMPTY
        );
        try {
            uf.compute();
        } catch (IllegalStateException e) {
            assertSame(error, e);
        }
    }

    private static final class FlakyGraph implements HugeGraph {
        private final int nodes;
        private final int maxDegree;
        private final Random random;
        private final RuntimeException error;

        private FlakyGraph(int nodes, int maxDegree, Random random, RuntimeException error) {
            this.nodes = nodes;
            this.maxDegree = maxDegree;
            this.random = random;
            this.error = error;
        }

        @Override
        public void canRelease(final boolean canRelease) {
        }

        @Override
        public long nodeCount() {
            return nodes;
        }

        @Override
        public void forEachRelationship(
                final long nodeId,
                final Direction direction,
                final HugeRelationshipConsumer consumer) {
            if (nodeId == 0) {
                throw error;
            }
            int degree = random.nextInt(maxDegree);
            int[] targets = IntStream.range(0, degree)
                    .map(i -> random.nextInt(nodes))
                    .filter(i -> i != nodeId)
                    .distinct()
                    .toArray();
            for (int target : targets) {
                if (!consumer.accept(nodeId, target)) {
                    break;
                }
            }
        }

        @Override
        public void forEachRelationship(
                final int nodeId,
                final Direction direction,
                final RelationshipConsumer consumer) {
            forEachRelationship((long) nodeId, direction, (s, t) -> consumer.accept((int) s, (int) t, -1L));
        }

        @Override
        public RelationshipIntersect intersection() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.intersection is not implemented.");
        }

        @Override
        public Collection<PrimitiveLongIterable> hugeBatchIterables(final int batchSize) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.hugeBatchIterables is not implemented.");
        }

        @Override
        public int degree(final long nodeId, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.degree is not implemented.");
        }

        @Override
        public long toHugeMappedNodeId(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.toHugeMappedNodeId is not implemented.");
        }

        @Override
        public long toOriginalNodeId(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.toOriginalNodeId is not implemented.");
        }

        @Override
        public boolean contains(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.contains is not implemented.");
        }

        @Override
        public void forEachNode(final LongPredicate consumer) {

        }

        @Override
        public PrimitiveLongIterator hugeNodeIterator() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.hugeNodeIterator is not implemented.");
        }

        @Override
        public HugeWeightMapping hugeNodeProperties(final String type) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.hugeNodeProperties is not implemented.");
        }

        @Override
        public long getTarget(final long nodeId, final long index, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.getTarget is not implemented.");
        }

        @Override
        public void forEachRelationship(
                final long nodeId, final Direction direction, final HugeWeightedRelationshipConsumer consumer) {

        }

        @Override
        public boolean exists(final long sourceNodeId, final long targetNodeId, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.exists is not implemented.");
        }

        @Override
        public double weightOf(final long sourceNodeId, final long targetNodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.weightOf is not implemented.");
        }

        @Override
        public Set<String> availableNodeProperties() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.availableNodeProperties is not implemented.");
        }

        @Override
        public int getTarget(final int nodeId, final int index, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.getTarget is not implemented.");
        }

        @Override
        public boolean exists(final int sourceNodeId, final int targetNodeId, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.ParallelUnionFindQueueTest.FlakyGraph.exists is not implemented.");
        }

        @Override
        public void forEachRelationship(
                final int nodeId, final Direction direction, final WeightedRelationshipConsumer consumer) {

        }
    }
}
