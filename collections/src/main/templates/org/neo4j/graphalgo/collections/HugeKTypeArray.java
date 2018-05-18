package org.neo4j.graphalgo.collections;

import org.neo4j.graphalgo.collections.cursors.*;
import org.neo4j.graphalgo.collections.functions.*;

import java.util.Arrays;
import java.util.function.IntFunction;


/**
 * A long-indexable version of a primitive ${TemplateOptions.KType.Type} array ({@code ${TemplateOptions.KType.Type}[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller ${TemplateOptions.KType.Type}-arrays ({@code ${TemplateOptions.KType.Type}[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code ${TemplateOptions.KType.Type}[]} might be used.
 * <p>
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link Sparse${TemplateOptions.KType.BoxedType}Array} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code ${TemplateOptions.KType.Type}[]} does ({@code 0} or {@code null}).</li>
 * </ul>
 * <p>
 * <h3>Basic Usage</h3>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * Huge${TemplateOptions.KType.BoxedType}Array array = Huge${TemplateOptions.KType.BoxedType}ArrayFactory.newArray(arraySize, tracker);
 * array.set(13L, 37);
 * ${TemplateOptions.KType.Type} value = array.get(13L);
 * // value = 37
 * {@code}
 * </pre>
 *
 * @author phorn@avantgarde-labs.de
 */
/*! ${TemplateOptions.generatedAnnotation} !*/
public interface HugeKTypeArray<KType> {

    /**
     * @return the long value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    KType get(long index);

    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    void set(long index, KType value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    void or(long index, KType value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     * @return the now current value after the operation
     */
    long and(long index, KType value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    void addTo(long index, KType value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(Object[], IntFunction)}.
     */
    <T extends LongToKTypeFunction<? extends KType>> void setAll(T gen);

    /**
     * Assigns the specified long value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(long[], long)}.
     */
    void fill(KType value);

    /**
     * Copies the content of this array into the target array.
     * <p>
     * The behavior is identical to {@link System#arraycopy(Object, int, Object, int, int)}.
     */
    void copyTo(final HugeKTypeArray<KType> dest, long length);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    long size();

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * The amount is not removed from the {@link AllocationTracker} that had been provided in the {@link HugeKTypeArrayFactory#newArray(long, AllocationTracker) Constructor}.
     *
     * @return the amount of memory freed, in bytes.
     */
    long release();


    /**
     * Returns a new {@link HugeKTypeCursor} for this array. The cursor is not positioned and in an invalid state.
     * You must call {@link HugeKTypeCursor#next()} first to position the cursor to a valid state.
     * Obtaining a {@link HugeKTypeCursor} for an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    HugeKTypeCursor<KType> newCursor();

    /**
     * Resets the {@link HugeKTypeCursor} to range from index 0 until {@link #size()}.
     * The returned cursor is not positioned and in an invalid state.
     * You must call {@link HugeKTypeCursor#next()} first to position the cursor to a valid state.
     * The returned cursor might be the reference-same ({@code ==}) one as the provided one.
     * Resetting the {@link HugeKTypeCursor} of an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    HugeKTypeCursor<KType> cursor(HugeKTypeCursor<KType> cursor);
}
