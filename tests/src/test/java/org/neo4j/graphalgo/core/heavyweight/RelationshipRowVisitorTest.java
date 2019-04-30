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


import org.junit.Test;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationshipRowVisitorTest {
    @Test
    public void byDefaultDontRemoveDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, 0d, false, AllocationTracker.EMPTY);
        IdMap idMap = idMap();

        Result.ResultRow row1 = mock(Result.ResultRow.class);
        when(row1.getNumber("source")).thenReturn(0L);
        when(row1.getNumber("target")).thenReturn(1L);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap, false, 0d, matrix, DuplicateRelationshipsStrategy.NONE);
        visitor.visit(row1);
        visitor.visit(row1);

        assertEquals(2, matrix.degree(0, Direction.OUTGOING));
    }

    @Test
    public void skipRemovesDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, false, 0d, false, AllocationTracker.EMPTY);

        Result.ResultRow row = mock(Result.ResultRow.class);
        when(row.getNumber("source")).thenReturn(0L);
        when(row.getNumber("target")).thenReturn(1L);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap(), false, 0d, matrix, DuplicateRelationshipsStrategy.SKIP);
        visitor.visit(row);
        visitor.visit(row);

        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
    }

    @Test
    public void sumRemovesDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, true, 0d, false, AllocationTracker.EMPTY);

        Result.ResultRow row = mock(Result.ResultRow.class);
        when(row.getNumber("source")).thenReturn(0L);
        when(row.getNumber("target")).thenReturn(1L);
        when(row.get("weight")).thenReturn(3.0, 7.0);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap(), true, 0d, matrix, DuplicateRelationshipsStrategy.SUM);
        visitor.visit(row);
        visitor.visit(row);

        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(10.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    @Test
    public void minRemovesDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, true, 0d, false, AllocationTracker.EMPTY);

        Result.ResultRow row = mock(Result.ResultRow.class);
        when(row.getNumber("source")).thenReturn(0L);
        when(row.getNumber("target")).thenReturn(1L);
        when(row.get("weight")).thenReturn(3.0, 7.0);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap(), true, 0d, matrix, DuplicateRelationshipsStrategy.MIN);
        visitor.visit(row);
        visitor.visit(row);

        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(3.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    @Test
    public void maxRemovesDuplicates() {
        AdjacencyMatrix matrix = new AdjacencyMatrix(2, true, 0d, false, AllocationTracker.EMPTY);

        Result.ResultRow row = mock(Result.ResultRow.class);
        when(row.getNumber("source")).thenReturn(0L);
        when(row.getNumber("target")).thenReturn(1L);
        when(row.get("weight")).thenReturn(3.0, 7.0);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap(), true, 0d, matrix, DuplicateRelationshipsStrategy.MAX);
        visitor.visit(row);
        visitor.visit(row);

        assertEquals(1, matrix.degree(0, Direction.OUTGOING));
        assertEquals(7.0, matrix.getOutgoingWeight(0, matrix.outgoingIndex(0, 1)), 0.01);
    }

    private IdMap idMap() {
        IdMap idMap = new IdMap(2);
        idMap.add(0);
        idMap.add(1);
        idMap.buildMappedIds(AllocationTracker.EMPTY);
        return idMap;
    }
}
