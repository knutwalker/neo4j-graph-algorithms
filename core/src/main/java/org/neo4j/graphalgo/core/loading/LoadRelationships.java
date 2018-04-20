package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;

import java.util.function.Consumer;

public interface LoadRelationships {

    int degreeOut(NodeCursor cursor);

    int degreeIn(NodeCursor cursor);

    int degreeBoth(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsOut(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsIn(NodeCursor cursor);

    RelationshipSelectionCursor relationshipsBoth(NodeCursor cursor);

    default int degreeOf(Direction direction, NodeCursor cursor) {
        switch (direction) {
            case OUTGOING:
                return degreeOut(cursor);
            case INCOMING:
                return degreeIn(cursor);
            case BOTH:
                return degreeBoth(cursor);
            default:
                throw new IllegalArgumentException("direction " + direction);
        }
    }

    default RelationshipSelectionCursor relationshipsOf(Direction direction, NodeCursor cursor) {
        switch (direction) {
            case OUTGOING:
                return relationshipsOut(cursor);
            case INCOMING:
                return relationshipsIn(cursor);
            case BOTH:
                return relationshipsBoth(cursor);
            default:
                throw new IllegalArgumentException("direction " + direction);
        }
    }

    static void consumeRelationships(RelationshipSelectionCursor cursor, Consumer<RelationshipSelectionCursor> action) {
        try (RelationshipSelectionCursor rels = cursor) {
            while (rels.next()) {
                action.accept(rels);
            }
        }
    }

    static LoadRelationships of(CursorFactory cursors, int[] relationshipType) {
        if (relationshipType == null || relationshipType.length == 0) {
            return new LoadAllRelationships(cursors);
        }
        return new LoadRelationshipsOfSingleType(cursors, relationshipType[0]);
    }
}


final class LoadAllRelationships implements LoadRelationships {
    private final CursorFactory cursors;

    LoadAllRelationships(final CursorFactory cursors) {
        this.cursors = cursors;
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor, cursors);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor, cursors);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
        return Nodes.countAll(cursor, cursors);
    }

    @Override
    public RelationshipSelectionCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, null);
    }

    @Override
    public RelationshipSelectionCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, null);
    }

    @Override
    public RelationshipSelectionCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, null);
    }
}

final class LoadRelationshipsOfSingleType implements LoadRelationships {
    private final CursorFactory cursors;
    private final int type;
    private final int[] types;

    LoadRelationshipsOfSingleType(final CursorFactory cursors, final int type) {
        this.cursors = cursors;
        this.type = type;
        this.types = new int[] {type};
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor, cursors, type);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor, cursors, type);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
        return Nodes.countAll(cursor, cursors, type);
    }

    @Override
    public RelationshipSelectionCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, types);
    }

    @Override
    public RelationshipSelectionCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, types);
    }

    @Override
    public RelationshipSelectionCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, types);
    }
}
