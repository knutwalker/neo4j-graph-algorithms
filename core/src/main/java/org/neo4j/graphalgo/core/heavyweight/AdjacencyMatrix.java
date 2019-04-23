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
package org.neo4j.graphalgo.core.heavyweight;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.IntPredicate;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.LINEAR_SEARCH_LIMIT;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.binarySearch;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.linearSearch;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

/**
 * Relation Container built of multiple arrays. The node capacity must be constant and the node IDs have to be
 * smaller then the capacity. The number of relations per node is limited only to the maximum array size of the VM
 * and connections can be added dynamically.
 *
 * @author mknblch
 */
public class AdjacencyMatrix {

    private static final int[] EMPTY_INTS = new int[0];

    /**
     * mapping from nodeId to outgoing degree
     */
    private final int[] outOffsets;
    /**
     * mapping from nodeId to incoming degree
     */
    private final int[] inOffsets;
    /**
     * matrix nodeId x [outgoing edge-relationIds..]
     */
    private final int[][] outgoing;
    /**
     * matrix nodeId x [incoming edge-relationIds..]
     */
    private final int[][] incoming;
    /**
     * list of weights per nodeId, encoded as {@link Float#floatToIntBits(float).
     * Representation is sparse, missing values are written as 0
     */
    private final int[][] outgoingWeights;
    /**
     * list of weights per nodeId, encoded as {@link Float#floatToIntBits(float).
     * Representation is sparse, missing values are written as 0
     */
    private final int[][] incomingWeights;
    private final double defaultWeight;

    private boolean sorted = false;

    private final AllocationTracker tracker;

    public AdjacencyMatrix(int nodeCount, boolean sorted, AllocationTracker tracker) {
        // TODO: pass weight loading (cypher constructor)
        this(nodeCount, true, true, false, 0.0, sorted, true, tracker);
    }

    AdjacencyMatrix(
            int nodeCount,
            boolean withIncoming,
            boolean withOutgoing,
            boolean withWeights,
            double defaultWeight,
            boolean sorted,
            boolean preFill,
            AllocationTracker tracker) {
        long allocated = 0L;
        if (withOutgoing) {
            allocated += sizeOfIntArray(nodeCount);
            allocated += sizeOfObjectArray(nodeCount);
            this.outOffsets = new int[nodeCount];
            this.outgoing = new int[nodeCount][];
            if (preFill) {
                Arrays.fill(outgoing, EMPTY_INTS);
            }
            if (withWeights) {
                allocated += sizeOfObjectArray(nodeCount);
                this.outgoingWeights = new int[nodeCount][];
                if (preFill) {
                    Arrays.fill(outgoingWeights, EMPTY_INTS);
                }
            } else {
                this.outgoingWeights = null;
            }
        } else {
            this.outOffsets = null;
            this.outgoing = null;
            this.outgoingWeights = null;
        }
        if (withIncoming) {
            allocated += sizeOfIntArray(nodeCount);
            allocated += sizeOfObjectArray(nodeCount);
            this.inOffsets = new int[nodeCount];
            this.incoming = new int[nodeCount][];
            if (preFill) {
                Arrays.fill(incoming, EMPTY_INTS);
            }
            if (withWeights) {
                allocated += sizeOfObjectArray(nodeCount);
                this.incomingWeights = new int[nodeCount][];
                if (preFill) {
                    Arrays.fill(incomingWeights, EMPTY_INTS);
                }
            } else {
                this.incomingWeights = null;
            }
        } else {
            this.inOffsets = null;
            this.incoming = null;
            this.incomingWeights = null;
        }
        tracker.add(allocated);
        this.defaultWeight = defaultWeight;
        this.sorted = sorted;
        this.tracker = tracker;
    }

    /**
     * initialize array for outgoing connections
     */
    public int[] armOut(int sourceNodeId, int degree) {
        if (degree > 0) {
            tracker.add(sizeOfIntArray(degree));
            outgoing[sourceNodeId] = new int[degree];
            if (outgoingWeights != null) {
                tracker.add(sizeOfIntArray(degree));
                outgoingWeights[sourceNodeId] = new int[degree];
            }
        }
        return outgoing[sourceNodeId];
    }

    /**
     * initialize array for incoming connections
     */
    public int[] armIn(int targetNodeId, int degree) {
        if (degree > 0) {
            tracker.add(sizeOfIntArray(degree));
            incoming[targetNodeId] = new int[degree];
            if (incomingWeights != null) {
                tracker.add(sizeOfIntArray(degree));
                incomingWeights[targetNodeId] = new int[degree];
            }
        }
        return incoming[targetNodeId];
    }

    /**
     * get weight storage for incoming weights
     */
    public int[] getInWeights(int targetNodeId) {
        if (incomingWeights != null) {
            return incomingWeights[targetNodeId];
        }
        return null;
    }

    /**
     * get weight storage for outgoing weights
     */
    public int[] getOutWeights(int targetNodeId) {
        if (outgoingWeights != null) {
            return outgoingWeights[targetNodeId];
        }
        return null;
    }

    void setOutDegree(int nodeId, final int degree) {
        outOffsets[nodeId] = degree;
    }

    void setInDegree(int nodeId, final int degree) {
        inOffsets[nodeId] = degree;
    }

    /**
     * grow array for outgoing connections
     */
    private void growOut(int sourceNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (outgoing[sourceNodeId].length < length) {
            outgoing[sourceNodeId] = growArray(outgoing[sourceNodeId], length);
        }
    }

    /**
     * grow array for incoming connections
     */
    private void growIn(int targetNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (incoming[targetNodeId].length < length) {
            incoming[targetNodeId] = growArray(incoming[targetNodeId], length);
        }
    }

    private int[] growArray(int[] array, int length) {
        int newSize = ArrayUtil.oversize(length, RamUsageEstimator.NUM_BYTES_INT);
        tracker.remove(sizeOfIntArray(array.length));
        tracker.add(sizeOfIntArray(newSize));
        return Arrays.copyOf(array, newSize);
    }

    /**
     * add outgoing relation
     */
    public void addOutgoing(int sourceNodeId, int targetNodeId) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, nextDegree);
        outgoing[sourceNodeId][degree] = targetNodeId;
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * checks for outgoing target node
     */
    public boolean hasOutgoing(int sourceNodeId, int targetNodeId) {

        final int degree = outOffsets[sourceNodeId];
        final int[] rels = outgoing[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    public int getTargetOutgoing(int nodeId, int index) {
        final int degree = outOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return outgoing[nodeId][index];
    }

    public int getTargetIncoming(int nodeId, int index) {
        final int degree = inOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return incoming[nodeId][index];
    }

    public int getTargetBoth(int nodeId, int index) {
        final int outDegree = outOffsets[nodeId];
        if (index >= 0 && index < outDegree) {
            return outgoing[nodeId][index];
        } else {
            index -= outDegree;
            final int inDegree = inOffsets[nodeId];
            if (index >= 0 && index < inDegree) {
                return incoming[nodeId][index];
            }
        }
        return -1;
    }


    /**
     * checks for incoming target node
     */
    public boolean hasIncoming(int sourceNodeId, int targetNodeId) {

        final int degree = inOffsets[sourceNodeId];
        final int[] rels = incoming[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    /**
     * add incoming relation
     */
    public void addIncoming(int sourceNodeId, int targetNodeId) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        growIn(targetNodeId, nextDegree);
        incoming[targetNodeId][degree] = sourceNodeId;
        inOffsets[targetNodeId] = nextDegree;
    }

    /**
     * get the degree for node / direction
     *
     * @throws NullPointerException if the direction hasn't been loaded.
     */
    public int degree(int nodeId, Direction direction) {
        switch (direction) {
            case OUTGOING: {
                return outOffsets[nodeId];
            }
            case INCOMING: {
                return inOffsets[nodeId];
            }
            default: {
                return inOffsets[nodeId] + outOffsets[nodeId];
            }
        }
    }

    /**
     * iterate over each edge at the given node using an unweighted consumer
     */
    public void forEach(int nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachIncoming(nodeId, consumer);
                forEachOutgoing(nodeId, consumer);
                break;
        }
    }

    /**
     * iterate over each edge at the given node using a weighted consumer
     */
    public void forEach(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
                break;
        }
    }

    public double weightOf(final int sourceNodeId, final int targetNodeId) {
        if (outgoingWeights != null) {
            int[] weights = outgoingWeights[sourceNodeId];
            if (weights != null) {
                int[] outs = outgoing[sourceNodeId];
                int degree = Math.min(outOffsets[sourceNodeId], outs.length);
                for (int i = 0; i < degree; i++) {
                    if (outs[i] == targetNodeId) {
                        return Float.intBitsToFloat(weights[i]);
                    }
                }
            }
        }
        if (incomingWeights != null) {
            int[] weights = incomingWeights[targetNodeId];
            if (weights != null) {
                int[] ins = incoming[targetNodeId];
                int degree = Math.min(inOffsets[targetNodeId], ins.length);
                for (int i = 0; i < degree; i++) {
                    if (ins[i] == sourceNodeId) {
                        return Float.intBitsToFloat(weights[i]);
                    }
                }
            }
        }
        return defaultWeight;
    }

    public boolean hasWeights() {
        return outgoingWeights != null || incomingWeights != null;
    }

    public int capacity() {
        return outOffsets != null
                ? outOffsets.length
                : inOffsets != null
                ? inOffsets.length
                : 0;
    }

    private void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], RawValues.combineIntInt(nodeId, outs[i]));
        }
    }

    private void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, ins[i], RawValues.combineIntInt(ins[i], nodeId));
        }
    }

    private void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        int[] weights;
        if (outgoingWeights == null || ((weights = outgoingWeights[nodeId]) == null)) {
            forEachOutgoingDefaultWeight(nodeId, consumer);
            return;
        }
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(nodeId, outs[i]);
            consumer.accept(nodeId, outs[i], relationId, Float.intBitsToFloat(weights[i]));
        }
    }

    private void forEachOutgoingDefaultWeight(int nodeId, WeightedRelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(nodeId, outs[i]);
            consumer.accept(nodeId, outs[i], relationId, defaultWeight);
        }
    }

    private void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        int[] weights;
        if (incomingWeights == null || ((weights = incomingWeights[nodeId]) == null)) {
            forEachIncomingDefaultWeight(nodeId, consumer);
            return;
        }
        final int degree = inOffsets[nodeId];
        final int[] neighbours = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(neighbours[i], nodeId);
            consumer.accept(nodeId, neighbours[i], relationId, Float.intBitsToFloat(weights[i]));
        }
    }

    private void forEachIncomingDefaultWeight(int nodeId, WeightedRelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] neighbours = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            final long relationId = RawValues.combineIntInt(neighbours[i], nodeId);
            consumer.accept(nodeId, neighbours[i], relationId, defaultWeight);
        }
    }

    public NodeIterator nodesWithRelationships(Direction direction) {
        if (direction == Direction.OUTGOING) {
            return new DegreeCheckingNodeIterator(outOffsets);
        } else {
            return new DegreeCheckingNodeIterator(inOffsets);
        }
    }

    public void sortIncoming(int node) {
        // TODO: sort weights according to sort result of this
        Arrays.sort(incoming[node], 0, inOffsets[node]);
    }

    public void sortOutgoing(int node) {
        // TODO: sort weights according to sort result of this
        Arrays.sort(outgoing[node], 0, outOffsets[node]);
    }

    public void sortAll(ExecutorService pool, int concurrency) {
        // TODO: sort weights according to sort result of this
        ParallelUtil.iterateParallel(pool, outgoing.length, concurrency, node -> {
            sortIncoming(node);
            sortOutgoing(node);
        });
        sorted = true;
    }

    public void intersectAll(int nodeA, IntersectionConsumer consumer) {
        int outDegreeA = outOffsets[nodeA];
        int[] neighboursA = outgoing[nodeA];
        for (int i = 0; i < outDegreeA; i++) {
            int nodeB = neighboursA[i];
            int outDegreeB = outOffsets[nodeB];
            int[] neighboursB = outgoing[nodeB];
            int[] jointNeighbours = Intersections.getIntersection(neighboursA, outDegreeA, neighboursB, outDegreeB);
            for (int nodeC : jointNeighbours) {
                if (nodeB < nodeC) consumer.accept(nodeA,nodeB,nodeC);
            }
        }
    }

    private static class DegreeCheckingNodeIterator implements NodeIterator {

        private final int[] array;

        DegreeCheckingNodeIterator(int[] array) {
            this.array = array != null ? array : EMPTY_INTS;
        }

        @Override
        public void forEachNode(IntPredicate consumer) {
            for (int node = 0; node < array.length; node++) {
                if (array[node] > 0 && !consumer.test(node)) {
                    break;
                }
            }
        }

        @Override
        public PrimitiveIntIterator nodeIterator() {
            return new PrimitiveIntIterator() {
                int index = findNext();

                @Override
                public boolean hasNext() {
                    return index < array.length;
                }

                @Override
                public int next() {
                    try {
                        return index;
                    } finally {
                        index = findNext();
                    }
                }

                private int findNext() {
                    int length = array.length;
                    for (int n = index + 1; n < length; n++) {
                        if (array[n] > 0) {
                            return n;
                        }
                    }
                    return length;
                }
            };
        }
    }
}
