package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipIterator;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

final class WeightedComputeStep extends BaseComputeStep implements WeightedRelationshipConsumer {
    private final double[] aggregatedDegrees;
    private final WeightedRelationshipIterator relationshipIterator;
    private double sumOfWeights;
    private double delta;

    WeightedComputeStep(
            double dampingFactor,
            int[] sourceNodeIds,
            WeightedRelationshipIterator weightedRelationshipIterator,
            Degrees degrees,
            int partitionSize,
            int startNode,
            DegreeCache degreeCache) {
        super(dampingFactor,
                sourceNodeIds,
                degrees,
                partitionSize,
                startNode);
        this.relationshipIterator = weightedRelationshipIterator;
        this.aggregatedDegrees = degreeCache.aggregatedDegrees();
    }

    void singleIteration() {
        int startNode = this.startNode;
        int endNode = this.endNode;
        WeightedRelationshipIterator rels = this.relationshipIterator;
        for (int nodeId = startNode; nodeId < endNode; ++nodeId) {
            delta = deltas[nodeId - startNode];
            if (delta > 0) {
                int degree = degrees.degree(nodeId, Direction.OUTGOING);
                if (degree > 0) {
                    sumOfWeights = aggregatedDegrees[nodeId];

                    rels.forEachRelationship(nodeId, Direction.OUTGOING, this);
                }
            }
        }
    }

    @Override
    public boolean accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
        if (weight > 0) {
            double proportion = weight / sumOfWeights;
            int srcRankDelta = (int) (100_000 * (delta * proportion));
            if (srcRankDelta != 0) {
                int idx = binaryLookup(targetNodeId, starts);
                nextScores[idx][targetNodeId - starts[idx]] += srcRankDelta;
            }
        }

        return true;
    }
}
