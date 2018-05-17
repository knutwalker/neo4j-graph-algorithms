package org.neo4j.graphalgo.collections;

import org.neo4j.graphalgo.collections.cursors.*;
import org.neo4j.graphalgo.collections.functions.*;

import java.util.Arrays;

/*! ${TemplateOptions.generatedAnnotation} !*/
final class SingleHugeKTypeArray<KType> implements HugeKTypeArray<KType> {

    private static final int PAGE_SHIFT = 30;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static long memoryOfArray(int length) {
        /*! #if ($templateOnly) !*/ return 0L;
        /*! #else return MemoryUsage.sizeOf${TemplateOptions.KType.BoxedType}Array(length); #end !*/
    }

    static <KType> SingleHugeKTypeArray<KType> of(long size, AllocationTracker tracker) {
        assert size <= PAGE_SIZE;
        final int intSize = (int) size;
        KType[] page = Intrinsics.<KType>newArray(intSize);

        tracker.add(MemoryUsage.shallowSizeOfInstance(SingleHugeKTypeArray.class));
        tracker.add(memoryOfArray(intSize));

        return new SingleHugeKTypeArray<KType>(intSize, page);
    }

    private final int size;
    private /*! #if ($TemplateOptions.KTypePrimitive)
        KType[] #else !*/
        Object[] /*! #end !*/
            page;

    private SingleHugeKTypeArray(
            int size,
            /*! #if ($TemplateOptions.KTypePrimitive)
            KType[] #else !*/
            Object[] /*! #end !*/
            page) {
        this.size = size;
        this.page = page;
    }

    @Override
    public KType get(long index) {
        assert index < size;
        return Intrinsics.<KType>cast(page[(int) index]);
    }

    @Override
    public void set(long index, KType value) {
        assert index < size;
        page[(int) index] = value;
    }

    @Override
    public void or(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive && (($TemplateOptions.KType.Type ne "double") && ($TemplateOptions.KType.Type ne "float")))
        page[(int) index] |= value; #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/

    }

    @Override
    public long and(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive && (($TemplateOptions.KType.Type ne "double") && ($TemplateOptions.KType.Type ne "float")))
        return page[(int) index] &= value; #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/
    }

    @Override
    public void addTo(long index, KType value) {
        assert index < size;
        /*! #if ($TemplateOptions.KTypePrimitive)
        page[(int) index] += value; #else !*/
        throw new UnsupportedOperationException();
        /*! #end !*/
    }

    @Override
    public <T extends LongToKTypeFunction<? extends KType>> void setAll(final T gen) {
        KType[] page = Intrinsics.<KType[]>cast(this.page);
        for (int i = 0; i < page.length; i++) {
            page[i] = gen.apply(i);
        }
    }

    @Override
    public void fill(KType value) {
        Arrays.fill(page, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void copyTo(HugeKTypeArray dest, long length) {
        if (length > size) {
            length = size;
        }
        if (length > dest.size()) {
            length = dest.size();
        }
        if (dest instanceof SingleHugeKTypeArray) {
            SingleHugeKTypeArray<KType> dst = (SingleHugeKTypeArray<KType>) dest;
            System.arraycopy(page, 0, dst.page, 0, (int) length);
            Arrays.fill(dst.page, (int) length, dst.size, Intrinsics.empty());
        } else if (dest instanceof PagedHugeKTypeArray) {
            PagedHugeKTypeArray<KType> dst = (PagedHugeKTypeArray<KType>) dest;
            dst.copyFrom(Intrinsics.<KType[]>cast(page), (int) length);
        }
    }

    void copyFrom(KType[] page, int offset, int length) {
        System.arraycopy(page, 0, this.page, offset, length);
    }

    void clearTail(int from) {
        Arrays.fill(page, from, size, Intrinsics.empty());
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long release() {
        if (page != null) {
            page = null;
            return MemoryUsage.sizeOfLongArray(size);
        }
        return 0L;
    }

    @Override
    public HugeKTypeCursor<KType> newCursor() {
        return new SingleCursor<KType>(Intrinsics.<KType[]>cast(page));
    }

    @Override
    public HugeKTypeCursor<KType> cursor(final HugeKTypeCursor<KType> cursor) {
        assert cursor instanceof SingleCursor;
        ((SingleCursor) cursor).init();
        return cursor;
    }

    private static final class SingleCursor<KType> extends HugeKTypeCursor<KType> {

        private boolean exhausted;

        private SingleCursor(final KType[] page) {
            super();
            this.array = page;
            this.base = 0L;
            this.limit = page.length;
        }

        private void init() {
            exhausted = false;
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
}
