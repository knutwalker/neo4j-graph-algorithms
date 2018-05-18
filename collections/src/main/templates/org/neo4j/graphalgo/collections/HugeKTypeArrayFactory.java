package org.neo4j.graphalgo.collections;

/*! ${TemplateOptions.generatedAnnotation} !*/
public final class HugeKTypeArrayFactory<KType> {

    /**
     * Creates a new array if the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static <KType> HugeKTypeArray<KType> newArray(long size, AllocationTracker tracker) {
        if (size <= SingleHugeKTypeArray.PAGE_SIZE) {
            try {
                return SingleHugeKTypeArray.of(size, tracker);
            } catch (OutOfMemoryError ignored) {
                // OOM is very likely because we just tried to create a single array that is too large
                // in which case we're just going the paged way. If the OOM had any other reason, we're
                // probably triggering it again in the construction of the paged array, where it will be thrown.
            }
        }
        return PagedHugeKTypeArray.of(size, tracker);
    }

    /* test-visible */
    static <KType> HugeKTypeArray<KType> newSingleArray(long size, AllocationTracker tracker) {
        return SingleHugeKTypeArray.of(size, tracker);
    }

    /* test-visible */
    static <KType> HugeKTypeArray<KType> newPagedArray(long size, AllocationTracker tracker) {
        return PagedHugeKTypeArray.of(size, tracker);
    }

    private HugeKTypeArrayFactory() {
    }
}
