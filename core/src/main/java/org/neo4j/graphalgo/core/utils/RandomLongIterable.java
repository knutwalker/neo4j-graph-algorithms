package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * iterates over a range of long values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * A {@link BitSet} is used to store the seen values and guarantee that no
 * number is produced twice. The memory requirements are therefore
 * {@code (end - start) / 8} bytes for the bit set and 64 or so bytes for
 * the instance state.
 *
 * If created with {@link #RandomLongIterable(long, long, Random, boolean)} and {@code true} as last parameter,
 * the iterator returned on every call to {@link #iterator()} will be the same
 * and will provide the same iteration order and cannot be shared.
 */
public final class RandomLongIterable implements PrimitiveLongIterable {

    private final long start;
    private final long end;
    private final Random random;
    private final RandomLongIterator iterator;

    /**
     * @param end iteration end, exclusive
     */
    public RandomLongIterable(long end) {
        this(0L, end, ThreadLocalRandom.current(), false);
    }

    /**
     * @param start iteration start, inclusive
     * @param end   iteration end, exclusive
     */
    public RandomLongIterable(long start, long end) {
        this(start, end, ThreadLocalRandom.current(), false);
    }

    /**
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomLongIterable(long end, Random random) {
        this(0L, end, random, false);
    }

    /**
     * @param start         iteration start, inclusive
     * @param end           iteration end, exclusive
     * @param random        random instance to provide the initial seed
     * @param reuseIterator if true, the iterator returned on every call to {@link #iterator()} will be the same
     *                      and will provide the same iteration order and cannot be shared.
     */
    public RandomLongIterable(long start, long end, Random random, boolean reuseIterator) {
        this.start = start;
        this.end = end;
        this.random = random;
        iterator = reuseIterator ? new RandomLongIterator(start, end, random) : null;
    }

    @Override
    public PrimitiveLongIterator iterator() {
        RandomLongIterator iterator = this.iterator;
        if (iterator == null) {
            iterator = new RandomLongIterator(start, end, random);
        } else {
            iterator.reset();
        }
        return iterator;
    }
}
