package org.neo4j.graphalgo.core.huge;

import org.apache.lucene.util.LongsRef;
import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.MarshlandPool;
import org.neo4j.collection.pool.Pool;

final class ImporterObjectPool {

    private final Pool<LongsRef> bufferPool;

    ImporterObjectPool() {
        this.bufferPool = new MarshlandPool<>(new LinkedQueuePool<>(1, LongsRef::new));
    }

    LongsRef getBuffer() {
        return bufferPool.acquire();
    }

    void returnBuffer(LongsRef buffer) {
        bufferPool.release(buffer);
    }
}
