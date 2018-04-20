package org.neo4j.graphalgo.core.utils;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class StatementAction extends StatementApi implements RenamesCurrentThread, Runnable, StatementApi.Consumer {

    protected StatementAction(GraphDatabaseAPI api) {
        super(api);
    }

    @Override
    public void run() {
        Runnable revertName = RenamesCurrentThread.renameThread(threadName());
        try {
            acceptInTransaction(this);
        } catch (Exception e) {
            Exceptions.throwIfUnchecked(e);
            throw new RuntimeException(e);
        } finally {
            revertName.run();
        }
    }
}
