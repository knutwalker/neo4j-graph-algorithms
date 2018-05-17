package org.neo4j.graphalgo.collections.cursors;

/**
 * View of the underlying data, accessible as slices of {@code long[]} arrays.
 * The values are from {@code array[offset]} (inclusive) until {@code array[limit]} (exclusive).
 * The range might match complete array, but that isn't guaranteed.
 * <p>
 * The {@code limit} parameter does not have the same meaning as the {@code length} parameter that is used in many methods that can operate on array slices.
 * The proper value would be {@code int length = limit - offset}.
 */
/*! ${TemplateOptions.generatedAnnotation} !*/
public abstract class HugeKTypeCursor<KType> implements AutoCloseable {

    /** the base for the index to get the global index */
    public long base;
    /** a slice of values currently being traversed */
    public KType[] array;
    /** the offset into the array */
    public final int offset = 0;
    /** the limit of the array, exclusive – the first index not to be contained */
    public int limit;

    /**
     * Try to load the next page and return the success of this load.
     * Once the method returns {@code false}, this method will never return {@code true} again until the cursor is reset using {@code #cursor()}.
     * The cursor behavior is not defined and might be unusable and throw exceptions after this method returns {@code false}.
     *
     * @return true, iff the cursor is still valid on contains new data; false if there is no more data.
     */
    public boolean next() {
        return false;
    }

    /**
     * Releases the reference to the underlying array so that it might be garbage collected.
     * The cursor can never be used again after calling this method, doing so results in undefined behavior.
     */
    @Override
    public void close() {
    }
}
