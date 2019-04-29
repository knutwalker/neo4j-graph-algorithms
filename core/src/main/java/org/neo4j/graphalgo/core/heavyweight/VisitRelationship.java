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

import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;

import java.util.Arrays;


abstract class VisitRelationship {

    private final IdMap idMap;
    private final boolean shouldSort;

    private int[] targets;
    private int[] weights;
    private int length;
    private long prevNode;
    private boolean isSorted;

    private int prevTarget;
    private int sourceGraphId;

    VisitRelationship(final IdMap idMap, final boolean shouldSort) {
        this.idMap = idMap;
        this.shouldSort = shouldSort;
        if (!shouldSort) {
            isSorted = false;
        }
    }

    abstract void visit(RelationshipSelectionCursor cursor);

    final void prepareNextNode(final int sourceGraphId, final int[] targets, final int[] weights) {
        this.sourceGraphId = sourceGraphId;
        length = 0;
        prevTarget = -1;
        prevNode = -1L;
        isSorted = shouldSort;
        this.targets = targets;
        this.weights = weights;
    }

    final void prepareNextNode(VisitRelationship other) {
        this.sourceGraphId = other.sourceGraphId;
        length = other.length;
        prevTarget = other.prevTarget;
        prevNode = other.prevNode;
        isSorted = other.isSorted;
        targets = other.targets;
        weights = other.weights;
    }

    final boolean addNode(final long nodeId) {
        if (nodeId == prevNode) {
            return false;
        }
        final int targetId = idMap.get(nodeId);
        if (targetId == -1) {
            return false;
        }
        if (isSorted && targetId < prevTarget) {
            isSorted = false;
        }
        targets[length++] = targetId;
        prevNode = nodeId;
        prevTarget = targetId;
        return true;
    }

    final int flush() {
        if (shouldSort && !isSorted) {
            if (weights != null) {
                int[] targets = this.targets;
                int[] weights = this.weights;
                int length = Math.min(this.length, Math.min(targets.length, weights.length));
                // TODO: reuse and resize
                // TODO: write into this array during import
                //       though this here might actually benefit from vectorization
                long[] targetsAndWeights = new long[length];
                for (int i = 0; i < length; i++) {
                    targetsAndWeights[i] = ((long) targets[i] << 32) | (long) weights[i] & 0xFFFFFFFFL;
                }
                // we need to sort only the targets but have to reorder the weights accordingly
                // by putting them both into longs with the target id at the more significant position
                // we can sort those longs and it will only sort by id and "drag along" the weights.
                // This is done because there is no indirect sort option in j.u.Arrays and the
                // IndirectSort provided by HPPC does allocate an (or possibly multiple) index array(s)
                // and sorts by indirect lookup into the value array.
                Arrays.sort(targetsAndWeights, 0, length);
                int write = 0;
                for (int i = 0, prev = -1; i < length; i++) {
                    long targetAndWeight = targetsAndWeights[i];
                    int target = (int) (targetAndWeight >> 32);
                    if (target > prev) {
                        weights[write] = (int) targetAndWeight;
                        targets[write++] = target;
                        prev = target;
                    }
                }
                this.length = write;
            } else {
                Arrays.sort(targets, 0, length);
                length = checkDistinct(targets, length);
            }
        }
        return length;
    }

    final void visitWeight(
            Read readOp,
            CursorFactory cursors,
            int propertyId,
            double defaultValue,
            long relationshipId,
            long propertyReference) {
        assert weights != null : "loaded weight but no weight loading was specified at construction";
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            readOp.relationshipProperties(relationshipId, propertyReference, pc);
            double weight = ReadHelper.readProperty(pc, propertyId, defaultValue);
            weights[length - 1] = Float.floatToRawIntBits((float) weight);
        }
    }

    private static int checkDistinct(final int[] values, final int len) {
        int prev = -1;
        for (int i = 0; i < len; i++) {
            final int value = values[i];
            if (value == prev) {
                return distinct(values, i, len);
            }
            prev = value;
        }
        return len;
    }

    private static int distinct(final int[] values, final int start, final int len) {
        int prev = values[start - 1];
        int write = start;
        for (int i = start + 1; i < len; i++) {
            final int value = values[i];
            if (value > prev) {
                values[write++] = value;
            }
            prev = value;
        }
        return write;
    }
}

final class VisitOutgoingNoWeight extends VisitRelationship {

    VisitOutgoingNoWeight(final IdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.targetNodeReference());
    }
}

final class VisitIncomingNoWeight extends VisitRelationship {

    VisitIncomingNoWeight(final IdMap idMap, final boolean shouldSort) {
        super(idMap, shouldSort);
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        addNode(cursor.sourceNodeReference());
    }
}

final class VisitOutgoingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final double defaultValue;
    private final int propertyId;

    VisitOutgoingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final IdMap idMap,
            final boolean shouldSort,
            final int propertyId,
            final double defaultValue) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.cursors = cursors;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.targetNodeReference())) {
            visitWeight(readOp, cursors, propertyId, defaultValue, cursor.relationshipReference(), cursor.propertiesReference());
        }
    }
}

final class VisitIncomingWithWeight extends VisitRelationship {

    private final Read readOp;
    private final CursorFactory cursors;
    private final double defaultValue;
    private final int propertyId;

    VisitIncomingWithWeight(
            final Read readOp,
            final CursorFactory cursors,
            final IdMap idMap,
            final boolean shouldSort,
            final int propertyId,
            final double defaultValue) {
        super(idMap, shouldSort);
        this.readOp = readOp;
        this.cursors = cursors;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    @Override
    void visit(final RelationshipSelectionCursor cursor) {
        if (addNode(cursor.sourceNodeReference())) {
            visitWeight(readOp, cursors, propertyId, defaultValue, cursor.relationshipReference(), cursor.propertiesReference());
        }
    }
}
