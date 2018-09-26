package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.PaddedAtomicLong;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;

public final class PageCacheScanner {

    public static final class Cursor implements AutoCloseable, RelationshipDataAccessor {

        private final int type;
        private final long maxId;
        private final int recordSize;
        private final int pageSize;
        private final int prefetchSize;
        private final long maxPage; // inclusive, last page to contain a value in range

        private ThreadLocal<Cursor> cursors;
        private AtomicLong nextPageId;
        private RecordFormat<RelationshipRecord> recordFormat;
        private RelationshipRecord record;
        private PageCursor pageCursor;

        private long recordId;
        private long currentPage;
        private long prefetchedUntilPage;
        private int offset;
        private int endOffset;

        private Cursor(
                int type,
                long maxId,
                int recordSize,
                int pageSize,
                int recordsPerPage,
                int prefetchSize,
                ThreadLocal<Cursor> cursors,
                AtomicLong nextPageId,
                RecordFormat<RelationshipRecord> recordFormat,
                RelationshipRecord record,
                PageCursor pageCursor) {
            this.type = type;
            this.maxId = maxId;
            this.recordSize = recordSize;
            this.pageSize = pageSize;
            this.prefetchSize = prefetchSize;
            this.maxPage = ((maxId - 1L) + ((long) recordsPerPage - 1L)) / (long) recordsPerPage;
            this.cursors = cursors;
            this.nextPageId = nextPageId;
            this.recordFormat = recordFormat;
            this.record = record;
            this.pageCursor = pageCursor;
            this.offset = pageSize; // trigger page load as first action
            this.endOffset = pageSize;
        }

        public boolean next() {
            if (recordId == -1L) {
                return false;
            }

            try {
                do {
                    if (loadFromCurrentPage()) {
                        return true;
                    }

                    if (loadNextPage()) {
                        continue;
                    }

                    record.setId(recordId = -1L);
                    record.clear();
                    return false;
                } while (true);
            } catch (IOException e) {
                throw new UnderlyingStorageException(e);
            }
        }

        private boolean loadFromCurrentPage() throws IOException {
            while (offset < endOffset) {
                record.setId(recordId++);
                loadAtOffset(offset);
                offset += recordSize;
                if (isWantedTypeAndInUse()) {
                    return true;
                }
            }
            return false;
        }

        private boolean loadNextPage() throws IOException {
            long current = currentPage++;
            if (current < prefetchedUntilPage) {
                offset = 0;
                return pageCursor.next(current);
            }
            if (current < maxPage) {
                preFetchPages();
                return loadNextPage();
            }
            if (current == maxPage) {
                offset = 0;
                endOffset = offsetForId(maxId, pageSize, recordSize);
                return pageCursor.next(current);
            }
            return false;
        }

        private void preFetchPages() throws IOException {
            PageCursor pageCursor = this.pageCursor;
            long prefetchSize = (long) this.prefetchSize;
            long startPage = nextPageId.getAndAdd(prefetchSize);
            long endPage = Math.min(maxPage, startPage + prefetchSize);
            long preFetchedPage = startPage;
            while (preFetchedPage < endPage) {
                if (!pageCursor.next(preFetchedPage)) {
                    break;
                }
                ++preFetchedPage;
            }
            this.currentPage = startPage;
            this.prefetchedUntilPage = preFetchedPage;
        }

        private void loadAtOffset(int offset) throws IOException {
            do {
                record.setInUse(false);
                pageCursor.setOffset(offset);
                recordFormat.read(record, pageCursor, RecordLoad.CHECK, recordSize);
            } while (pageCursor.shouldRetry());
            verifyLoad();
        }

        private void verifyLoad() {
            pageCursor.checkAndClearBoundsFlag();
            // TODO: needed?
//            pageCursor.clearCursorException();
//            if (!record.inUse()) {
//                record.clear();
//            }
        }

//        private void throwOutOfBoundsException(long recordId, long pageId, int offset) {
//            final RelationshipRecord record = recordFormat.newRecord();
//            record.setId(recordId);
//            throw new UnderlyingStorageException(String.format(
//                    "Access to relationship record %s went out of bounds of the page. The record size is %d bytes, and the access was at offset %d bytes into page %d.",
//                    record,
//                    recordSize,
//                    offset,
//                    pageId));
//        }

        private boolean isWantedTypeAndInUse() {
            return record.inUse() && ((type & record.getType()) == record.getType());
        }

        @Override
        public long relationshipReference() {
            return record.getId();
        }

        @Override
        public int type() {
            return record.getType();
        }

        @Override
        public long sourceNodeReference() {
            return record.getFirstNode();
        }

        @Override
        public long targetNodeReference() {
            return record.getSecondNode();
        }

        @Override
        public long propertiesReference() {
            return record.getNextProp();
        }


        @Override
        public boolean hasProperties() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.PageCacheScanner.Cursor.hasProperties is not implemented.");
        }

        @Override
        public void source(final NodeCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.PageCacheScanner.Cursor.source(NodeCursor) is not implemented.");
        }

        @Override
        public void target(final NodeCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.PageCacheScanner.Cursor.target(NodeCursor) is not implemented.");
        }

        @Override
        public void properties(final PropertyCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.PageCacheScanner.Cursor.properties(PropertyCursor) is not implemented.");
        }

        @Override
        public void close() {
            if (pageCursor != null) {
                pageCursor.close();
                pageCursor = null;
                record = null;
                recordFormat = null;
                nextPageId = null;

                final Cursor localCursor = cursors.get();
                // sanity check, should always be called from the same thread
                if (localCursor == this) {
                    cursors.remove();
                }
                cursors = null;
            }
        }
    }

    private final int type;
    private final int prefetchSize;
    private final RelationshipStore store;
    private final RecordFormat<RelationshipRecord> recordFormat;
    private final long maxId;
    private final int recordsPerPage;
    private final int recordSize;
    private final int pageSize;
    private final ThreadLocal<Cursor> cursors;
    private final AtomicLong nextPageId;

    public PageCacheScanner(GraphDatabaseAPI api, int prefetchSize, int type) {
        NeoStores neoStores = api
                .getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();
        this.type = type;
        this.prefetchSize = prefetchSize;
        store = neoStores.getRelationshipStore();
        recordFormat = neoStores.getRecordFormats().relationship();
        maxId = 1L + store.getHighestPossibleIdInUse();
        recordsPerPage = store.getRecordsPerPage();
        recordSize = store.getRecordSize();
        pageSize = recordsPerPage * recordSize;
        cursors = new ThreadLocal<>();
        nextPageId = new PaddedAtomicLong();
    }

    public Cursor getCursor() {
        Cursor cursor = cursors.get();
        if (cursor == null) {
            // Don't add as we want to always call next as the first cursor action,
            // which actually does the advance and returns the correct cursor.
            // This is just to position the page cursor somewhere in the vicinity
            // of its actual next page.
            long next = nextPageId.get();
            PageCursor pageCursor = store.openPageCursorForReading(next * recordsPerPage);
            RelationshipRecord record = store.newRecord();
            cursor = new Cursor(
                    type,
                    maxId,
                    recordSize,
                    pageSize,
                    recordsPerPage,
                    prefetchSize,
                    cursors,
                    nextPageId,
                    recordFormat,
                    record,
                    pageCursor);
            cursors.set(cursor);
        }
        return cursor;
    }

//    PageCacheScanner reset() {
//        return new PageCacheScanner(type, relStore, maxId, pageSize);
//    }
}
