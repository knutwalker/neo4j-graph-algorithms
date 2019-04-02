package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;

public final class HugeArraysTest {
    @Test
    public void pageIndexOfFirstPage() {
        assertEquals(0L, pageIndex(0L));
        assertEquals(0L, pageIndex(1L));
        assertEquals(0L, pageIndex(PAGE_SIZE - 1));
    }

    @Test
    public void pageIndexOfSecondPage() {
        assertEquals(1L, pageIndex(PAGE_SIZE));
        assertEquals(1L, pageIndex(2 * PAGE_SIZE - 1));
    }

    @Test
    public void pageIndexOfMaxPage() {
        assertEquals(Integer.MAX_VALUE, pageIndex((long) Integer.MAX_VALUE * PAGE_SIZE));
        assertEquals(Integer.MAX_VALUE, pageIndex((long) Integer.MAX_VALUE * PAGE_SIZE + PAGE_SIZE - 1L));
    }

    @Test
    public void pageIndexUndefinedWhenIndexIsTooLarge() {
        assertEquals(0L, pageIndex(1L << 62));
        assertEquals(-1L, pageIndex(Long.MAX_VALUE));
    }

    @Test
    public void pageIndexUndefinedForNegativeIndex() {
        assertEquals(-1L, pageIndex(-1L));
        assertEquals(-1L, pageIndex(-2L));
        assertEquals(0L, pageIndex(Long.MIN_VALUE));
    }

    @Test
    public void indexInPageOnFirstPage() {
        assertEquals(0L, indexInPage(0L));
        assertEquals(1L, indexInPage(1L));
        assertEquals(PAGE_SIZE - 1L, indexInPage(PAGE_SIZE - 1L));
    }

    @Test
    public void indexInPageOnSecondPage() {
        assertEquals(0L, indexInPage(PAGE_SIZE));
        assertEquals(1L, indexInPage(PAGE_SIZE + 1L));
        assertEquals(PAGE_SIZE - 1L, indexInPage(2 * PAGE_SIZE - 1L));
    }

    @Test
    public void indexInPageOfMaxPage() {
        assertEquals(0L, indexInPage((long) Integer.MAX_VALUE * PAGE_SIZE));
        assertEquals(PAGE_SIZE - 1L, indexInPage((long) Integer.MAX_VALUE * PAGE_SIZE + PAGE_SIZE - 1L));
    }

    @Test
    public void indexInPageWhenIndexIsTooLarge() {
        assertEquals(0L, indexInPage(1L << 62));
        assertEquals(PAGE_SIZE - 1L, indexInPage(Long.MAX_VALUE));
    }

    @Test
    public void indexInPageForNegativeIndex() {
        assertEquals(PAGE_SIZE - 1L, indexInPage(-1L));
        assertEquals(PAGE_SIZE - 2L, indexInPage(-2L));
        assertEquals(0L, indexInPage(Long.MIN_VALUE));
    }

    @Test
    public void exclusiveIndexOfPageOnFirstPage() {
        assertEquals(1L, exclusiveIndexOfPage(1L));
        assertEquals(PAGE_SIZE - 1L, exclusiveIndexOfPage(PAGE_SIZE - 1L));
    }

    @Test
    public void exclusiveIndexDoesNotJumpToNextPage() {
        assertEquals(PAGE_SIZE, exclusiveIndexOfPage(0L));
        assertEquals(PAGE_SIZE, exclusiveIndexOfPage(PAGE_SIZE));
    }

    @Test
    public void numberOfPagesForNoData() {
        assertEquals(0L, numberOfPages(0L));
    }

    @Test
    public void numberOfPagesForOnePage() {
        assertEquals(1L, numberOfPages(1L));
        assertEquals(1L, numberOfPages(PAGE_SIZE));
    }

    @Test
    public void numberOfPagesForMorePages() {
        assertEquals(2L, numberOfPages(PAGE_SIZE + 1L));
        assertEquals(2L, numberOfPages(2 * PAGE_SIZE));
    }

    @Test
    public void numberOfPagesForMaxPages() {
        assertEquals(Integer.MAX_VALUE, numberOfPages((long) Integer.MAX_VALUE * PAGE_SIZE));
    }

    @Test
    public void numberOfPagesForTooManyPages() {
        testNumberOfPagesForTooManyPages(Long.MAX_VALUE);
        testNumberOfPagesForTooManyPages((long) Integer.MAX_VALUE * PAGE_SIZE + 1L);
    }

    private void testNumberOfPagesForTooManyPages(final long capacity) {
        try {
            numberOfPages(capacity);
            fail("capacity should have been too large");
        } catch (AssertionError e) {
            assertEquals("pageSize=" + PAGE_SIZE + " is too small for capacity: " + capacity, e.getMessage());
        }
    }
}
