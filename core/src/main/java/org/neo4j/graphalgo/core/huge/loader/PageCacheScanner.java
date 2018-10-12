/**
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.paged.PaddedAtomicLong;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;

public final class PageCacheScanner {

    public static final class Cursor implements AutoCloseable, RelationshipDataAccessor {

        // size in bytes of a single record - advance the offset by this much
        private final int recordSize;
        // how many records are there in a single page
        private final int recordsPerPage;

        // fetch this many pages at once
        private final int prefetchSize;
        // last page to contain a value of interest, inclusive, but we mostly
        // treat is as exclusive since we want to special-case the last page
        private final long lastPage;
        // end offset of the last page, exclusive (first offset to be out-of-range)
        private final int lastOffset;

        // global pointer which block of pages need to be fetched next
        private AtomicLong nextPageId;
        // global cursor pool to return this one to
        private ThreadLocal<Cursor> cursors;
        // how to read the record
        private RecordFormat<RelationshipRecord> recordFormat;
        // thread-local cursor instance
        private PageCursor pageCursor;
        // thread-local record instance
        private RelationshipRecord record;

        // the current record id -
        private long recordId;
        // the current page
        private long currentPage;
        // the last page that has already been fetched - exclusive
        private long fetchedUntilPage;
        // the current offset into the page
        private int offset;
        // the end offset of the current page - exclusive
        private int endOffset;

        private Cursor(
                int recordSize,
                int recordsPerPage,
                int prefetchSize,
                long maxId,
                int pageSize,
                AtomicLong nextPageId,
                ThreadLocal<Cursor> cursors,
                RecordFormat<RelationshipRecord> recordFormat,
                PageCursor pageCursor,
                RelationshipRecord record) {
            this.recordSize = recordSize;
            this.recordsPerPage = recordsPerPage;
            this.prefetchSize = prefetchSize;
            this.lastPage = Math.max((((maxId - 1L) + ((long) recordsPerPage - 1L)) / (long) recordsPerPage) - 1L, 0L);
            this.lastOffset = offsetForId(maxId, pageSize, recordSize);
            this.cursors = cursors;
            this.nextPageId = nextPageId;
            this.recordFormat = recordFormat;
            this.record = record;
            this.pageCursor = pageCursor;
            this.offset = pageSize; // trigger page load as first action
            this.endOffset = pageSize;
        }

        int bulkSize() {
            return prefetchSize * recordsPerPage;
        }

        public boolean next(Predicate<RelationshipRecord> filter) {
            if (recordId == -1L) {
                return false;
            }

            try {
                do {
                    if (loadFromCurrentPage(filter)) {
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

        int bulkNext(long[] buffer, Predicate<RelationshipRecord> filter) {
            try {
                return bulkNext0(buffer, filter);
            } catch (IOException e) {
                throw new UnderlyingStorageException(e);
            }
        }

        private int bulkNext0(long[] buffer, Predicate<RelationshipRecord> filter) throws IOException {
            if (recordId == -1L) {
                return 0;
            }

            int endOffset;
            long page;
            long endPage;
            if (currentPage == lastPage) {
                page = lastPage;
                endOffset = lastOffset;
                endPage = 1L + page;
            } else if (currentPage > lastPage) {
                this.recordId = -1L;
                return 0;
            } else {
                preFetchPages();
                page = currentPage;
                endPage = fetchedUntilPage;
                endOffset = this.endOffset;
            }

            int outpos = 0;
            int offset = this.offset;
            long recordId = this.recordId;
            long perPage = (long) recordsPerPage;
            int recordSize = this.recordSize;
            PageCursor pageCursor = this.pageCursor;
            RelationshipRecord record = this.record;

            while (page < endPage) {
                if (!pageCursor.next(page++)) {
                    break;
                }
                offset = 0;
                recordId = page * perPage;

                while (offset < endOffset) {
                    record.setId(recordId++);
                    loadAtOffset(offset);
                    offset += recordSize;
                    if (record.inUse() && filter.test(record)) {
                        buffer[outpos] = record.getFirstNode();
                        buffer[1 + outpos] = record.getSecondNode();
                        buffer[2 + outpos] = record.getId();
                        buffer[3 + outpos] = record.getNextProp();
                        outpos += 4;
                    }
                }
            }

            currentPage = page;
            this.offset = offset;
            this.recordId = recordId;

            return outpos;
        }

        private boolean loadFromCurrentPage(Predicate<RelationshipRecord> filter) throws IOException {
            while (offset < endOffset) {
                record.setId(recordId++);
                loadAtOffset(offset);
                offset += recordSize;
                if (record.inUse() && filter.test(record)) {
                    return true;
                }
            }
            return false;
        }

        private boolean loadNextPage() throws IOException {
            long current = currentPage++;
            if (current < fetchedUntilPage) {
                offset = 0;
                recordId = current * recordsPerPage;
                return pageCursor.next(current);
            }
            if (current < lastPage) {
                preFetchPages();
                return loadNextPage();
            }
            if (current == lastPage) {
                offset = 0;
                endOffset = lastOffset;
                recordId = current * recordsPerPage;
                return pageCursor.next(current);
            }
            return false;
        }

        private void preFetchPages() throws IOException {
            PageCursor pageCursor = this.pageCursor;
            long prefetchSize = (long) this.prefetchSize;
            long startPage = nextPageId.getAndAdd(prefetchSize);
            long endPage = Math.min(lastPage, startPage + prefetchSize);
            long preFetchedPage = startPage;
            while (preFetchedPage < endPage) {
                if (!pageCursor.next(preFetchedPage)) {
                    break;
                }
                ++preFetchedPage;
            }
            this.currentPage = startPage;
            this.fetchedUntilPage = preFetchedPage;
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
                    "org.neo4j.graphalgo.core.huge.loader.PageCacheScanner.Cursor.hasProperties is not implemented.");
        }

        @Override
        public void source(final NodeCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.loader.PageCacheScanner.Cursor.source(NodeCursor) is not implemented.");
        }

        @Override
        public void target(final NodeCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.loader.PageCacheScanner.Cursor.target(NodeCursor) is not implemented.");
        }

        @Override
        public void properties(final PropertyCursor cursor) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.core.huge.loader.PageCacheScanner.Cursor.properties(PropertyCursor) is not implemented.");
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

    private final int recordSize;
    private final int recordsPerPage;
    private final int prefetchSize;
    private final long maxId;
    private final int pageSize;
    private final RecordFormat<RelationshipRecord> recordFormat;
    private final RelationshipStore store;
    private final PagedFile pagedFile;
    private final AtomicLong nextPageId;
    private final ThreadLocal<Cursor> cursors;


    private static final int DEFAULT_PREFETCH_SIZE = 100;
    private static final String REL_STORE_FILENAME = StoreFile.RELATIONSHIP_STORE.storeFileName();

    public static PageCacheScanner of(GraphDatabaseAPI api) {
        return of(api, DEFAULT_PREFETCH_SIZE);
    }

    public static PageCacheScanner of(GraphDatabaseAPI api, int prefetchSize) {
        NeoStores neoStores = api
                .getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores();

        RelationshipStore store = neoStores.getRelationshipStore();
        RecordFormat<RelationshipRecord> recordFormat = neoStores.getRecordFormats().relationship();
        int recordSize = store.getRecordSize();
        long maxId = 1L + store.getHighestPossibleIdInUse();

        PageCache pageCache = api
                .getDependencyResolver()
                .resolveDependency(PageCache.class);
        try {
            for (PagedFile pagedFile : pageCache.listExistingMappings()) {
                if (pagedFile.file().getName().equals(REL_STORE_FILENAME)) {
                    int pageSize = pagedFile.pageSize();
                    int recordsPerPage = pageSize / recordSize;
                    return new PageCacheScanner(
                            recordSize, recordsPerPage, prefetchSize, maxId,
                            pageSize, recordFormat, store, pagedFile);
                }
            }
        } catch (IOException ignored) {
        }

        int recordsPerPage = store.getRecordsPerPage();
        int pageSize = recordsPerPage * recordSize;
        return new PageCacheScanner(
                recordSize, recordsPerPage, prefetchSize, maxId,
                pageSize, recordFormat, store, null);
    }

    private PageCacheScanner(
            int recordSize,
            int recordsPerPage,
            int prefetchSize,
            long maxId,
            int pageSize,
            RecordFormat<RelationshipRecord> recordFormat,
            RelationshipStore store,
            PagedFile pagedFile) {
        this.recordSize = recordSize;
        this.recordsPerPage = recordsPerPage;
        this.prefetchSize = prefetchSize;
        this.maxId = maxId;
        this.pageSize = pageSize;
        this.recordFormat = recordFormat;
        this.store = store;
        this.pagedFile = pagedFile;
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

            PageCursor pageCursor;
            try {
                if (pagedFile != null) {
                    pageCursor = pagedFile.io(next, PagedFile.PF_READ_AHEAD | PagedFile.PF_SHARED_READ_LOCK);
                } else {
                    long recordId = next * (long) recordSize;
                    pageCursor = store.openPageCursorForReading(recordId);
                }
            } catch (IOException e) {
                throw new UnderlyingStorageException(e);
            }
            RelationshipRecord record = store.newRecord();
            cursor = new Cursor(
                    recordSize, recordsPerPage, prefetchSize, maxId, pageSize,
                    nextPageId, cursors, recordFormat, pageCursor, record);
            cursors.set(cursor);
        }
        return cursor;
    }

    long storeSize() {
        if (pagedFile != null) {
            return pagedFile.file().length();
        }
        long relsInUse = 1L + store.getHighestPossibleIdInUse();
        long idsInPages = ((relsInUse + (recordsPerPage - 1L)) / recordsPerPage) * recordsPerPage;
        return idsInPages * (long) recordSize;
    }

//    PageCacheScanner reset() {
//        return new PageCacheScanner(type, relStore, maxId, pageSize);
//    }
}
