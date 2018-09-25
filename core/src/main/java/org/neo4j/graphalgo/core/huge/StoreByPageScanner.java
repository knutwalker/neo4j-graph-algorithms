package org.neo4j.graphalgo.core.huge;

import org.jctools.queues.SpmcArrayQueue;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.paged.PaddedAtomicLong;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public final class StoreByPageScanner implements Runnable {

    private final int type;
    private final RelationshipStore store;
    private final RecordFormat<RelationshipRecord> recordFormat;
    private final long maxId;
    private final int recordsPerPage;
    private final int recordSize;
    private final int pageSize;
    private final AtomicLong pagesRequested;
    private final AtomicLong loadedIndex;
    private final AtomicLong consumedIndex;
    private final SpmcArrayQueue<PageInfo> requestQueue;
    private final SpmcArrayQueue<PageInfo> loadedQueue;


    StoreByPageScanner(GraphDatabaseAPI api, int type) {
        NeoStores neoStores = api
                .getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();

        this.type = type;
        store = neoStores.getRelationshipStore();
        recordFormat = neoStores.getRecordFormats().relationship();
        maxId = 1L + store.getHighestPossibleIdInUse();
        recordsPerPage = store.getRecordsPerPage();
        recordSize = store.getRecordSize();
        pageSize = recordsPerPage * recordSize;
        pagesRequested = new PaddedAtomicLong();
        loadedIndex = new PaddedAtomicLong();
        consumedIndex = new PaddedAtomicLong();
        requestQueue = new SpmcArrayQueue<>(8);
        loadedQueue = new SpmcArrayQueue<>(8);
    }

    int request(int numberOfPages) {
        pagesRequested.addAndGet(numberOfPages);
        return 0;
    }

    @Override
    public void run() {
        requestQueue.fill(PageInfo::new);
        try (PageCursor pageCursor = store.openPageCursorForReading(0L)) {
            RelationshipRecord record = store.newRecord();
            final long pageSize = (long) this.recordsPerPage;
            final long maxId = this.maxId;
            final long maxPage = ((maxId - 1L) + (pageSize - 1L)) / pageSize;
            long pagesRead = 0L;
            long currentPage = 0L;
            long recordId = 0L;



            while (currentPage  < maxId) {
                PageInfo info;
                do {
                    info = requestQueue.poll();
                } while (info == null);

                currentPage = pagesRead++;
                recordId = currentPage * pageSize;

            }

            outer:
            while (true) {
                long requested = pagesRequested.getAndSet(0L);
                while (requested-- > 0L) {
                    currentPage = pagesRead++;
                    recordId = currentPage * pageSize;
                    if (recordId >= this.maxId) {
                        // TODO signal stop
                        break outer;
                    }
                    if (!pageCursor.next(currentPage)) {
                        break outer;
                    }
                    pageCursor.setOffset(0);
                    record.setId(recordId);
                    do {
                        record.setInUse(false);
                        recordFormat.read(record, pageCursor, RecordLoad.CHECK, recordSize);
                    } while (pageCursor.shouldRetry());
                    pageCursor.checkAndClearBoundsFlag();
                }

            }
        } catch (IOException e) {
            throw ExceptionUtil.asUnchecked(e);
        }
    }

    private static final class PageInfo {
        private long pageId;
        private long recordId;
    }
}
