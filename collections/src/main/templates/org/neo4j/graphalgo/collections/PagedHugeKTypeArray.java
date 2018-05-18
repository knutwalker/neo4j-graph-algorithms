package org.neo4j.graphalgo.collections;

import org.neo4j.graphalgo.collections.cursors.*;
import org.neo4j.graphalgo.collections.functions.*;

import java.util.Arrays;

@SuppressWarnings("unchecked")
/*! ${TemplateOptions.generatedAnnotation} !*/
final class PagedHugeKTypeArray<KType> implements HugeKTypeArray<KType> {

    private static final int PAGE_SHIFT = 14;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

    private static long memoryOfArray(int length) {
        /*! #if ($templateOnly) !*/ return 0L;
        /*! #else return MemoryUsage.sizeOf${TemplateOptions.KType.BoxedType}Array(length); #end !*/
    }

    static <KType> PagedHugeKTypeArray<KType> of(long size, AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
        /*! #if ($TemplateOptions.KTypePrimitive)
        KType[][] pages = new KType[numPages][];
            #else !*/
        Object[][] pages = new Object[numPages][];
        /*! #end !*/

        long memoryUsed = MemoryUsage.sizeOfObjectArray(numPages);
        final long pageBytes = memoryOfArray(PAGE_SIZE);

        for (int i = 0; i < numPages - 1; i++) {
            /*! #if ($TemplateOptions.KTypePrimitive)
            KType[] page = new KType[PAGE_SIZE];
                #else !*/
            Object[] page = new Object[PAGE_SIZE];
            /*! #end !*/
            memoryUsed += pageBytes;
            pages[i] = page;
        }

        final int lastPageSize = indexInPage(size);
        /*! #if ($TemplateOptions.KTypePrimitive)
        KType[] lastPage = new KType[lastPageSize];
            #else !*/
        Object[] lastPage = new Object[lastPageSize];
        /*! #end !*/
        pages[numPages - 1] = lastPage;
        memoryUsed += memoryOfArray(lastPageSize);

        tracker.add(MemoryUsage.shallowSizeOfInstance(PagedHugeKTypeArray.class));
        tracker.add(memoryUsed);

        return new PagedHugeKTypeArray<KType>(size, pages, memoryUsed);
    }

    private final long size;
    private final long memoryUsed;
    private
      /*! #if ($TemplateOptions.KTypePrimitive)
        KType[][]
          #else !*/
        Object[][]
      /*! #end !*/
         pages;


    private PagedHugeKTypeArray(
            long size,
            /*! #if ($TemplateOptions.KTypePrimitive)
            KType[][] pages,
                #else !*/
            Object[][] pages,
            /*! #end !*/
            long memoryUsed) {
        this.size = size;
        this.pages = pages;
        this.memoryUsed = memoryUsed;
    }

    @Override
    public KType get(long index) {
        assert index < size;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return Intrinsics.<KType>cast(pages[pageIndex][indexInPage]);
    }

    @Override
    public void set(long index, KType value) {
        assert index < size;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] = value;
    }

    @Override
    public void or(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive && (($TemplateOptions.KType.Type ne "double") && ($TemplateOptions.KType.Type ne "float")))
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] |= value;
            #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/
    }

    @Override
    public long and(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive && (($TemplateOptions.KType.Type ne "double") && ($TemplateOptions.KType.Type ne "float")))
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage] &= value;
            #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/
    }

    @Override
    public void addTo(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive)
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] += value;
            #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/
    }

    @Override
    public <T extends LongToKTypeFunction<? extends KType>> void setAll(T gen) {
        for (int i = 0; i < pages.length; i++) {
            final long t = ((long) i) << PAGE_SHIFT;
            KType[] page = Intrinsics.<KType[]>cast(pages[i]);
            for (int j = 0; j < page.length; j++) {
                page[j] = gen.apply(t + j);
            }
        }
    }

    @Override
    public void fill(KType value) {
        for (int i = 0; i < pages.length; i++) {
            KType[] page = Intrinsics.<KType[]>cast(pages[i]);
            Arrays.fill(page, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void copyTo(HugeKTypeArray<KType> dest, long length) {
        if (length > size) {
            length = size;
        }
        if (length > dest.size()) {
            length = dest.size();
        }
        if (dest instanceof SingleHugeKTypeArray) {
            SingleHugeKTypeArray<KType> dst = (SingleHugeKTypeArray) dest;
            int start = 0;
            int remaining = (int) length;
            for (int i = 0; i < pages.length; i++) {
                KType[] page = Intrinsics.<KType[]>cast(pages[i]);
                int toCopy = Math.min(remaining, page.length);
                if (toCopy == 0) {
                    break;
                }
                dst.copyFrom(page, start, toCopy);
                start += toCopy;
                remaining -= toCopy;
            }
            dst.clearTail(start);
        } else if (dest instanceof PagedHugeKTypeArray) {
            PagedHugeKTypeArray<KType> dst = (PagedHugeKTypeArray<KType>) dest;
            int pageLen = Math.min(pages.length, dst.pages.length);
            int lastPage = pageLen - 1;
            long remaining = length;
            for (int i = 0; i < lastPage; i++) {
                KType[] page = Intrinsics.<KType[]>cast(pages[i]);
                KType[] dstPage = Intrinsics.<KType[]>cast(dst.pages[i]);
                System.arraycopy(page, 0, dstPage, 0, page.length);
                remaining -= page.length;
            }
            if (remaining > 0) {
                System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, Intrinsics.empty());
            }
            for (int i = pageLen; i < dst.pages.length; i++) {
                Arrays.fill(dst.pages[i], Intrinsics.empty());
            }
        }
    }

    void copyFrom(KType[] src, int remaining) {
        int start = 0;
        int pageLen = pages.length;
        for (int i = 0; i < pageLen; i++) {
            KType[] dstPage = Intrinsics.<KType[]>cast(pages[i]);
            int toCopy = Math.min(remaining, dstPage.length);
            if (toCopy == 0) {
                Arrays.fill(dstPage, Intrinsics.empty());
            } else {
                System.arraycopy(src, start, dstPage, 0, toCopy);
                if (toCopy < dstPage.length) {
                    Arrays.fill(dstPage, toCopy, dstPage.length, Intrinsics.empty());
                }
                start += toCopy;
                remaining -= toCopy;
            }
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long release() {
        if (pages != null) {
            pages = null;
            return memoryUsed;
        }
        return 0L;
    }

    @Override
    public HugeKTypeCursor<KType> newCursor() {
        return new PagedCursor<KType>(size, pages);
    }

    @Override
    public HugeKTypeCursor<KType> cursor(final HugeKTypeCursor<KType> cursor) {
        assert cursor instanceof PagedCursor;
        ((PagedCursor) cursor).init();
        return cursor;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    private static final class PagedCursor<KType> extends HugeKTypeCursor<KType> {

        private int maxPage;
        private long capacity;
        private
          /*! #if ($TemplateOptions.KTypePrimitive)
                KType[][] pages;
              #else !*/
            Object[][] pages;
          /*! #end !*/

        private int page;
        private int fromPage;

        private PagedCursor(
                final long capacity,
                /*! #if ($TemplateOptions.KTypePrimitive)
                KType[][] pages
                #else !*/
                Object[][] pages
                /*! #end !*/
        ) {
            super();
            this.capacity = capacity;
            this.maxPage = pages.length - 1;
            this.pages = pages;
        }

        private void init() {
            fromPage = 0;
            array = Intrinsics.<KType[]>cast(pages[0]);
            base = 0L;
            limit = (int) Math.min(PAGE_SIZE, capacity);
            page = -1;
        }

        @Override
        public final boolean next() {
            int current = ++page;
            if (current == fromPage) {
                return true;
            }
            if (current > maxPage) {
                return false;
            }
            base += PAGE_SIZE;
            array = Intrinsics.<KType[]>cast(pages[current]);
            limit = array.length;
            return true;
        }

        @Override
        public void close() {
            array = null;
            pages = null;
            base = 0L;
            limit = 0;
            capacity = 0L;
            maxPage = -1;
            fromPage = -1;
            page = -1;
        }
    }
}
