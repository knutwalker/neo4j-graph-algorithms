package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.utils.container.TrackingIntDoubleHashMap;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

public final class PagedLongDoubleMap {

    private static final int PAGE_SHIFT = 14;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

    public static PagedLongDoubleMap of(long size, AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, PAGE_MASK);
        tracker.add(sizeOfObjectArray(numPages));
        TrackingIntDoubleHashMap[] pages = new TrackingIntDoubleHashMap[numPages];
        return new PagedLongDoubleMap(pages, tracker);
    }

    private final AllocationTracker tracker;
    private TrackingIntDoubleHashMap[] pages;

    private PagedLongDoubleMap(
            TrackingIntDoubleHashMap[] pages,
            AllocationTracker tracker) {
        this.pages = pages;
        this.tracker = tracker;
    }

    public double getOrDefault(long index, double defaultValue) {
        int pageIndex = pageIndex(index);
        if (pageIndex < pages.length) {
            IntDoubleMap page = pages[pageIndex];
            if (page != null) {
                int indexInPage = indexInPage(index);
                return page.getOrDefault(indexInPage, defaultValue);
            }
        }
        return defaultValue;
    }

    public void put(long index, double value) {
        int pageIndex = pageIndex(index);
        TrackingIntDoubleHashMap subMap = subMap(pageIndex);
        int indexInPage = indexInPage(index);
        subMap.putSync(indexInPage, value);
    }

    private TrackingIntDoubleHashMap subMap(int pageIndex) {
        if (pageIndex >= pages.length) {
            return growNewSubMap(pageIndex);
        }
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap != null) {
            return subMap;
        }
        return forceNewSubMap(pageIndex);
    }

    private synchronized TrackingIntDoubleHashMap growNewSubMap(int pageIndex) {
        if (pageIndex >= pages.length) {
            long allocated = sizeOfObjectArray(1 + pageIndex) - sizeOfObjectArray(pages.length);
            tracker.add(allocated);
            pages = Arrays.copyOf(pages, 1 + pageIndex);
        }
        return forceNewSubMap(pageIndex);
    }

    private synchronized TrackingIntDoubleHashMap forceNewSubMap(int pageIndex) {
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap == null) {
            subMap = new TrackingIntDoubleHashMap(tracker);
            pages[pageIndex] = subMap;
        }
        return subMap;
    }

    public long release() {
        if (pages != null) {
            TrackingIntDoubleHashMap[] pages = this.pages;
            this.pages = null;
            long released = sizeOfObjectArray(pages.length);
            for (TrackingIntDoubleHashMap page : pages) {
                if (page != null) {
                    released += page.instanceSize();
                }
            }
            tracker.remove(released);
            return released;
        }
        return 0L;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }
}
