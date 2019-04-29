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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;

import java.util.function.Supplier;

public enum DuplicateRelationshipsStrategy {
    NONE {
        public double merge(double runningTotal, double weight) {
            throw new UnsupportedOperationException();
        }
    },
    SKIP {
        public double merge(double runningTotal, double weight) {
            return runningTotal;
        }
    },
    SUM {
        public double merge(double runningTotal, double weight) {
            return runningTotal + weight;
        }
    },
    MIN {
        public double merge(double runningTotal, double weight) {
            return Math.min(runningTotal, weight);
        }
    },
    MAX {
        public double merge(double runningTotal, double weight) {
            return Math.max(runningTotal, weight);
        }
    };

    public abstract double merge(double runningTotal, double weight);

    public void handle(int source, int target, AdjacencyMatrix matrix, boolean hasRelationshipWeights, double defaultWeight, Supplier<Number> weightSupplier) {
        double thisWeight = defaultWeight;
        if (hasRelationshipWeights) {
            Number weight = weightSupplier.get();
            if (weight != null) {
                thisWeight = weight.doubleValue();
            }
        }
        handle(source, target, matrix, hasRelationshipWeights, thisWeight);
    }

    public void handle(int source, int target, AdjacencyMatrix matrix, boolean hasRelationshipWeights, double weight) {
        if (this == DuplicateRelationshipsStrategy.NONE) {
            if (hasRelationshipWeights) {
                matrix.addOutgoingWithWeight(source, target, weight);
            } else {
                matrix.addOutgoing(source, target);
            }
        } else {
            if (hasRelationshipWeights) {
                int relationshipIndex = matrix.outgoingIndex(source, target);
                if (relationshipIndex >= 0) {
                    double oldWeight = matrix.getOutgoingWeight(source, relationshipIndex);
                    double newWeight = this.merge(oldWeight, weight);
                    matrix.addOutgoingWeight(source, relationshipIndex, newWeight);
                } else {
                    matrix.addOutgoingWithWeight(source, target, weight);
                }
            } else {
                boolean hasRelationship = matrix.hasOutgoing(source, target);
                if (!hasRelationship) {
                    matrix.addOutgoing(source, target);
                }
            }
        }
    }
}
