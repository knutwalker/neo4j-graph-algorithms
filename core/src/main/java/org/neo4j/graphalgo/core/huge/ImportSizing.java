package org.neo4j.graphalgo.core.huge;

import static org.neo4j.graphalgo.core.utils.paged.BitUtil.previousPowerOfTwo;

final class ImportSizing {

    // batch size is used to pre-size multiple arrays, so it must fit in an integer
    // 1B elements might even be too much as arrays need to be allocated with
    // a consecutive chunk of memory
    // possible idea: retry with lower batch sizes if alloc hits an OOM?
    private static final long MAX_BATCH_SIZE = (long) previousPowerOfTwo(Integer.MAX_VALUE);

    private static final String TOO_MANY_PAGES_REQUIRED =
            "Importing %d nodes would need %d arrays of %d-long nested arrays each, which cannot be created.";

    private final int totalThreads;
    private final int importerThreads;
    private final int pageSize;
    private final int numberOfPages;

    private ImportSizing(int totalThreads, int importerThreads, int pageSize, int numberOfPages) {
        this.totalThreads = totalThreads;
        this.importerThreads = importerThreads;
        this.pageSize = pageSize;
        this.numberOfPages = numberOfPages;
    }

    static ImportSizing of(int concurrency, long nodeCount) {
        return determineBestThreadSize(nodeCount, (long) concurrency);
    }

    private static ImportSizing determineBestThreadSize(long nodeCount, long targetThreads) {
        // aim for 1:1 of scanner:importer threads
        long targetImporterThreads = Math.max(1L, targetThreads >> 1L);

        // try to get about 4 pages per importer thread (based on 1:1 sizing of scanner:importer)
        long pageSize = ceilDiv(nodeCount, targetThreads << 1);

        // page size must be a power of two
        pageSize = previousPowerOfTwo(pageSize);

        // page size must fit in an integer
        pageSize = Math.min(MAX_BATCH_SIZE, pageSize);

        // determine the actual number of pages required
        long numberOfPages = ceilDiv(nodeCount, pageSize);

        // if we need too many pages, try to increase the page size
        while (numberOfPages > MAX_BATCH_SIZE && pageSize <= MAX_BATCH_SIZE) {
            pageSize <<= 1L;
            numberOfPages = ceilDiv(nodeCount, pageSize);
        }

        if (numberOfPages > MAX_BATCH_SIZE || pageSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    String.format(TOO_MANY_PAGES_REQUIRED, nodeCount, numberOfPages, pageSize)
            );
        }

        // int casts are safe as all are < MAX_BATCH_SIZE
        return new ImportSizing(
                (int) targetThreads,
                (int) targetImporterThreads,
                (int) pageSize,
                (int) numberOfPages
        );
    }

    int numberOfThreads() {
        return totalThreads;
    }

    int numberOfImporterThreads() {
        return importerThreads;
    }

    int pageSize() {
        return pageSize;
    }

    int numberOfPages() {
        return numberOfPages;
    }

    private static long ceilDiv(long dividend, long divisor) {
        return 1L + (-1L + dividend) / divisor;
    }
}
