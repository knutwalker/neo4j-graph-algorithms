package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class BaseNodeImporter<T> extends StatementFunction<T> {

    private final ImportProgress progress;
    private final long nodeCount;
    private final int labelId;

    public BaseNodeImporter(GraphDatabaseAPI api, ImportProgress progress, long nodeCount, int labelId) {
        super(api);
        this.progress = progress;
        this.nodeCount = nodeCount;
        this.labelId = labelId;
    }

    @Override
    public final T apply(final KernelTransaction transaction) {
        final T mapping = newNodeMap(nodeCount);
        ReadHelper.readNodes(transaction.cursors(), transaction.dataRead(), labelId, (nodeId) -> {
            addNodeId(mapping, nodeId);
            progress.nodeProgress();
        });
        finish(mapping);
        progress.resetForRelationships();
        return mapping;
    }

    protected abstract T newNodeMap(long nodeCount);

    protected abstract void addNodeId(T map, long nodeId);

    protected abstract void finish(T map);
}
