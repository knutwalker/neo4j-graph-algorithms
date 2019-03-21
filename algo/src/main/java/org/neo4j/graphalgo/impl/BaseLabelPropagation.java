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

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;

abstract class BaseLabelPropagation<
        G extends Graph,
        W extends WeightMapping,
        Self extends BaseLabelPropagation<G, W, Self>
        > extends LabelPropagationAlgorithm<Self> {

    private static final long[] EMPTY_LONGS = new long[0];

    private G graph;
    private final long nodeCount;
    private final AllocationTracker tracker;
    private final W nodeProperties;
    private final W nodeWeights;
    final int batchSize;
    final int concurrency;
    final ExecutorService executor;

    private Labels labels;
    private long ranIterations;
    private boolean didConverge;

    BaseLabelPropagation(
            G graph,
            W nodeProperties,
            W nodeWeights,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.nodeWeights = nodeWeights;
    }

    abstract Labels initialLabels(long nodeCount, AllocationTracker tracker);


    abstract List<BaseStep> baseSteps(
            final G graph,
            final Labels labels,
            final W nodeProperties,
            final W nodeWeights,
            final Direction direction,
            final RandomProvider randomProvider);

    @Override
    Self compute(
            Direction direction,
            long maxIterations,
            RandomProvider random) {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        if (labels == null || labels.size() != nodeCount) {
            labels = initialLabels(nodeCount, tracker);
        }

        ranIterations = 0L;
        didConverge = false;

        List<BaseStep> baseSteps = baseSteps(graph, labels, nodeProperties, nodeWeights, direction, random);

        long currentIteration = 0L;
        while (running() && currentIteration < maxIterations) {
            ParallelUtil.runWithConcurrency(concurrency, baseSteps, terminationFlag, executor);
            ++currentIteration;
        }

        long maxIteration = 0L;
        boolean converged = true;
        for (BaseStep baseStep : baseSteps) {
            Step current = baseStep.current;
            if (current instanceof BaseLabelPropagation.Computation) {
                Computation step = (Computation) current;
                if (step.iteration > maxIteration) {
                    maxIteration = step.iteration;
                }
                converged = converged && !step.didChange;
                step.release();
            }
        }

        ranIterations = maxIteration;
        didConverge = converged;

        return me();
    }

    final BaseStep asStep(Initialization initialization) {
        return new BaseStep(initialization);
    }

    @Override
    public final long ranIterations() {
        return ranIterations;
    }

    @Override
    public final boolean didConverge() {
        return didConverge;
    }

    @Override
    public final Labels labels() {
        return labels;
    }

    @Override
    public Self release() {
        graph = null;
        return me();
    }

    static abstract class Initialization implements Step {
        abstract void setExistingLabels();

        abstract Computation computeStep();

        @Override
        public final void run() {
            setExistingLabels();
        }

        @Override
        public final Step next() {
            return computeStep();
        }
    }

    static abstract class Computation implements Step {

        private final Labels existingLabels;
        private final ProgressLogger progressLogger;
        private final double maxNode;
        private final LongDoubleHashMap votes;

        private boolean didChange = true;
        long iteration = 0L;

        Computation(
                final Labels existingLabels,
                final ProgressLogger progressLogger,
                final long maxNode,
                final RandomProvider randomProvider) {
            this.existingLabels = existingLabels;
            this.progressLogger = progressLogger;
            this.maxNode = (double) maxNode;
            if (randomProvider.isRandom()) {
                Random random = randomProvider.randomForNewIteration();
                this.votes = new LongDoubleHashMap(
                        DEFAULT_EXPECTED_ELEMENTS,
                        (double) DEFAULT_LOAD_FACTOR,
                        HashOrderMixing.constant(random.nextLong()));
            } else {
                this.votes = new LongDoubleScatterMap();
            }
        }

        abstract boolean computeAll();

        abstract void forEach(long nodeId);

        abstract double weightOf(long nodeId, long candidate);

        @Override
        public final void run() {
            if (this.didChange) {
                iteration++;
                didChange = computeAll();
                if (!didChange) {
                    release();
                }
            }
        }

        final boolean iterateAll(PrimitiveIntIterator nodeIds) {
            boolean didChange = false;
            while (nodeIds.hasNext()) {
                long nodeId = (long) nodeIds.next();
                didChange = compute(nodeId, didChange);
                progressLogger.logProgress((double) nodeId, maxNode);
            }
            return didChange;
        }

        final boolean iterateAll(PrimitiveLongIterator nodeIds) {
            boolean didChange = false;
            while (nodeIds.hasNext()) {
                long nodeId = nodeIds.next();
                didChange = compute(nodeId, didChange);
                progressLogger.logProgress((double) nodeId, maxNode);
            }
            return didChange;
        }

        final boolean compute(long nodeId, boolean didChange) {
            votes.clear();
            long partition = existingLabels.labelFor(nodeId);
            long previous = partition;
            forEach(nodeId);
            double weight = Double.NEGATIVE_INFINITY;
            for (LongDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    partition = vote.key;
                }
            }
            if (partition != previous) {
                existingLabels.setLabelFor(nodeId, partition);
                return true;
            }
            return didChange;
        }

        final void castVote(long nodeId, long candidate) {
            double weight = weightOf(nodeId, candidate);
            long partition = existingLabels.labelFor(candidate);
            votes.addTo(partition, weight);
        }

        @Override
        public final Step next() {
            return this;
        }

        final void release() {
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

    interface Step extends Runnable {
        @Override
        void run();

        Step next();
    }

    static final class BaseStep implements Runnable {

        private Step current;

        BaseStep(final Step current) {
            this.current = current;
        }

        @Override
        public void run() {
            current.run();
            current = current.next();
        }
    }

    static final class RandomlySwitchingIntIterable implements PrimitiveIntIterable {
        private final PrimitiveIntIterable inner;
        private final RandomProvider randomize;

        static PrimitiveIntIterable of(
                RandomProvider randomize,
                PrimitiveIntIterable delegate) {
            return randomize.isRandom()
                    ? new RandomlySwitchingIntIterable(delegate, randomize)
                    : delegate;
        }

        private RandomlySwitchingIntIterable(
                PrimitiveIntIterable inner,
                RandomProvider randomize) {
            this.inner = inner;
            this.randomize = randomize;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return new RandomlySwitchingIntIterator(inner.iterator(), randomize.randomForNewIteration());
        }
    }

    static final class RandomlySwitchingIntIterator implements PrimitiveIntIterator {
        private final PrimitiveIntIterator delegate;
        private final Random random;
        private boolean hasSkipped;
        private int skipped;

        private RandomlySwitchingIntIterator(PrimitiveIntIterator delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public boolean hasNext() {
            return hasSkipped || delegate.hasNext();
        }

        @Override
        public int next() {
            if (hasSkipped) {
                int elem = skipped;
                hasSkipped = false;
                return elem;
            }
            int next = delegate.next();
            if (delegate.hasNext() && random.nextBoolean()) {
                skipped = next;
                hasSkipped = true;
                return delegate.next();
            }
            return next;
        }
    }

    static final class RandomlySwitchingLongIterable implements PrimitiveLongIterable {
        private final PrimitiveLongIterable inner;
        private final RandomProvider randomize;

        static PrimitiveLongIterable of(
                RandomProvider randomize,
                PrimitiveLongIterable delegate) {
            return randomize.isRandom()
                    ? new RandomlySwitchingLongIterable(delegate, randomize)
                    : delegate;
        }

        private RandomlySwitchingLongIterable(
                PrimitiveLongIterable inner,
                RandomProvider randomize) {
            this.inner = inner;
            this.randomize = randomize;
        }

        @Override
        public PrimitiveLongIterator iterator() {
            return new RandomlySwitchingLongIterator(inner.iterator(), randomize.randomForNewIteration());
        }
    }

    static final class RandomlySwitchingLongIterator implements PrimitiveLongIterator {
        private final PrimitiveLongIterator delegate;
        private final Random random;
        private boolean hasSkipped;
        private long skipped;

        private RandomlySwitchingLongIterator(PrimitiveLongIterator delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public boolean hasNext() {
            return hasSkipped || delegate.hasNext();
        }

        @Override
        public long next() {
            if (hasSkipped) {
                long elem = skipped;
                hasSkipped = false;
                return elem;
            }
            long next = delegate.next();
            if (delegate.hasNext() && random.nextBoolean()) {
                skipped = next;
                hasSkipped = true;
                return delegate.next();
            }
            return next;
        }
    }
}
