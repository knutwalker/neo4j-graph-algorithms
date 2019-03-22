package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * iterates over a range of int values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * A {@link BitSet} is used to store the seen values and guarantee that no
 * number is produced twice. The memory requirements are therefore
 * {@code (end - start) / 8} bytes for the bit set and 64 or so bytes for
 * the instance state.
 *
 * If created with {@link #RandomIntIterable(int, int, Random, boolean)} and {@code true} as last parameter,
 * the iterator returned on every call to {@link #iterator()} will be the same
 * and will provide the same iteration order and cannot be shared.
 */
public final class RandomIntIterable implements PrimitiveIntIterable {

    private final int start;
    private final int end;
    private final Random random;
    private final RandomIntIterator iterator;

    /**
     * @param end iteration end, exclusive
     */
    public RandomIntIterable(int end) {
        this(0, end, ThreadLocalRandom.current(), false);
    }

    /**
     * @param start iteration start, inclusive
     * @param end   iteration end, exclusive
     */
    public RandomIntIterable(int start, int end) {
        this(start, end, ThreadLocalRandom.current(), false);
    }

    /**
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomIntIterable(int end, Random random) {
        this(0, end, random, false);
    }

    /**
     * @param start         iteration start, inclusive
     * @param end           iteration end, exclusive
     * @param random        random instance to provide the initial seed
     * @param reuseIterator if true, the iterator returned on every call to {@link #iterator()} will be the same
     *                      and will provide the same iteration order and cannot be shared.
     */
    public RandomIntIterable(int start, int end, Random random, boolean reuseIterator) {
        this.start = start;
        this.end = end;
        this.random = random;
        iterator = reuseIterator ? new RandomIntIterator(start, end, random) : null;
    }

    @Override
    public PrimitiveIntIterator iterator() {
        RandomIntIterator iterator = this.iterator;
        if (iterator == null) {
            iterator = new RandomIntIterator(start, end, random);
        } else {
            iterator.reset();
        }
        return iterator;
    }
}
