package org.neo4j.graphalgo.core.utils.paged;

import static java.lang.reflect.Array.getLength;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;

/**
 * View of data underlying an Huge array, accessible as slices of one or more primitive arrays.
 * The values are from {@code array[offset]} (inclusive) until {@code array[limit]} (exclusive).
 * The range might match the complete array, but that isn't guaranteed.
 * <p>
 * The {@code limit} parameter does not have the same meaning as the {@code length} parameter that is used in many methods that can operate on array slices.
 * The proper value would be {@code int length = limit - offset}.
 */
public abstract class HugeCursor<Array> implements AutoCloseable {

    /**
     * the base for the index to get the global index
     */
    public long base;
    /**
     * a slice of values currently being traversed
     */
    public Array array;
    /**
     * the offset into the array
     */
    public int offset;
    /**
     * the limit of the array, exclusive – the first index not to be contained
     */
    public int limit;

    HugeCursor() {
    }

    /**
     * Try to load the next page and return the success of this load.
     * Once the method returns {@code false}, this method will never return {@code true} again until the cursor is reset.
     * The cursor behavior is not defined and might be unusable and throw exceptions after this method returns {@code false}.
     *
     * @return true, iff the cursor is still valid on contains new data; false if there is no more data.
     */
    abstract public boolean next();

    /**
     * Releases the reference to the underlying array so that it might be garbage collected.
     * The cursor can never be used again after calling this method, doing so results in undefined behavior.
     */
    @Override
    abstract public void close();

    /**
     * initializes cursor from 0 to capacity
     */
    abstract void init();

    /**
     * initializes cursor from start to end
     */
    abstract void init(long start, long end);


    static final class SinglePageCursor<Array> extends HugeCursor<Array> {

        private boolean exhausted;

        SinglePageCursor(final Array page) {
            super();
            this.array = page;
            this.base = 0L;
        }

        @Override
        void init() {
            init(0, getLength(array));
        }

        @Override
        void init(final long start, final long end) {
            init((int) start, (int) end);
        }

        private void init(int start, int end) {
            exhausted = false;
            offset = start;
            limit = end;
        }

        @Override
        public final boolean next() {
            if (exhausted) {
                return false;
            }
            exhausted = true;
            return true;
        }

        @Override
        public void close() {
            array = null;
            limit = 0;
            exhausted = true;
        }
    }

    static final class PagedCursor<Array> extends HugeCursor<Array> {

        private Array[] pages;
        private int pageIndex;
        private int fromPage;
        private int maxPage;
        private long capacity;
        private long end;

        PagedCursor(final long capacity, final Array[] pages) {
            super();
            this.capacity = capacity;
            this.pages = pages;
        }

        @Override
        void init() {
            init(0L, capacity);
        }

        @Override
        void init(long start, long end) {
            fromPage = pageIndex(start);
            maxPage = pageIndex(end - 1L);
            pageIndex = fromPage - 1;
            this.end = end;
            base = (long) fromPage << PAGE_SHIFT;
            offset = indexInPage(start);
            limit = fromPage == maxPage ? exclusiveIndexOfPage(end) : PAGE_SIZE;
        }

        @Override
        public final boolean next() {
            int current = ++pageIndex;
            if (current > maxPage) {
                return false;
            }
            array = pages[current];
            if (current == fromPage) {
                return true;
            }
            base += PAGE_SIZE;
            offset = 0;
            limit = current == maxPage ? exclusiveIndexOfPage(end) : getLength(array);
            return true;
        }

        @Override
        public void close() {
            array = null;
            pages = null;
            base = 0L;
            end = 0L;
            limit = 0;
            capacity = 0L;
            maxPage = -1;
            fromPage = -1;
            pageIndex = -1;
        }
    }
}
