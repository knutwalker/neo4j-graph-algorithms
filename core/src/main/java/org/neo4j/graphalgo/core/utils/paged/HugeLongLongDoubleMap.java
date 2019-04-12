/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.Containers;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;

/**
 * map with two longs as keys and huge underlying storage, so it can
 * store more than 2B values
 */
public final class HugeLongLongDoubleMap {

    private static final long CLASS_MEMORY = shallowSizeOfInstance(HugeLongLongDoubleMap.class);

    private final AllocationTracker tracker;

    private HugeLongArray keys;
    private HugeDoubleArray values;
    private HugeCursor<long[]> keysCursor;

    private int keyMixer;
    private long assigned;
    private long mask;
    private long resizeAt;

    private static final long DEFAULT_EXPECTED_ELEMENTS = 4L;
    private static final double LOAD_FACTOR = 0.75;

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongDoubleMap(AllocationTracker tracker) {
        this(DEFAULT_EXPECTED_ELEMENTS, tracker);
    }

    /**
     * New instance with sane defaults.
     */
    public HugeLongLongDoubleMap(long expectedElements, AllocationTracker tracker) {
        this.tracker = tracker;
        initialBuffers(expectedElements);
        tracker.add(CLASS_MEMORY);
    }

    public long sizeOf() {
        return CLASS_MEMORY + keys.sizeOf() + values.sizeOf();
    }

    public void put(long key1, long key2, double value) {
        put0(1L + key1, 1L + key2, value);
    }

    public void addTo(long key1, long key2, double value) {
        addTo0(1L + key1, 1L + key2, value);
    }

    public double getOrDefault(long key1, long key2, double defaultValue) {
        return getOrDefault0(1L + key1, 1L + key2, defaultValue);
    }

    private void put0(long key1, long key2, double value) {
        assert assigned < mask + 1L;
        final long key = hashKey(key1, key2);

        long slot = findSlot(key1, key2, key & mask);
        assert slot != -1L;
        if (slot >= 0L) {
            values.set(slot, value);
            return;
        }

        slot = ~(1L + slot);
        if (assigned == resizeAt) {
            allocateThenInsertThenRehash(slot, key1, key2, value);
        } else {
            values.set(slot, value);
            keys.set(slot << 1, key1);
            keys.set((slot << 1) + 1, key2);
        }

        assigned++;
    }

    private void addTo0(long key1, long key2, double value) {
        assert assigned < mask + 1L;
        final long key = hashKey(key1, key2);

        long slot = findSlot(key1, key2, key & mask);
        assert slot != -1L;
        if (slot >= 0L) {
            values.addTo(slot, value);
            return;
        }

        slot = ~(1L + slot);
        if (assigned == resizeAt) {
            allocateThenInsertThenRehash(slot, key1, key2, value);
        } else {
            values.set(slot, value);
            keys.set(slot << 1, key1);
            keys.set((slot << 1) + 1, key2);
        }

        assigned++;
    }

    private double getOrDefault0(long key1, long key2, double defaultValue) {
        final long key = hashKey(key1, key2);

        long slot = findSlot(key1, key2, key & mask);
        if (slot >= 0L) {
            return values.get(slot);
        }

        return defaultValue;
    }

    private long findSlot(
            long key1,
            long key2,
            long start) {
        HugeLongArray keys = this.keys;
        HugeCursor<long[]> cursor = this.keysCursor;
        long slot = findSlot(key1, key2, start << 1, keys.size(), keys, cursor);
        if (slot == -1L) {
            slot = findSlot(key1, key2, 0L, start << 1, keys, cursor);
        }
        return slot;
    }

    private long findSlot(
            long key1,
            long key2,
            long start,
            long end,
            HugeLongArray keys,
            HugeCursor<long[]> cursor) {

        long slot = start >> 1;
        int blockPos, blockEnd;
        long[] keysBlock;
        long existing;
        keys.cursor(cursor, start, end);
        while (cursor.next()) {
            keysBlock = cursor.array;
            blockPos = cursor.offset;
            blockEnd = cursor.limit - 1;
            while (blockPos < blockEnd) {
                existing = keysBlock[blockPos];
                if (existing == key1 && keysBlock[blockPos + 1] == key2) {
                    return slot;
                }
                if (existing == 0L) {
                    return ~slot - 1L;
                }
                blockPos += 2;
                ++slot;
            }
        }
        return -1L;
    }

    public long size() {
        return assigned;
    }

    public boolean isEmpty() {
        return size() == 0L;
    }

    public void release() {
        long released = CLASS_MEMORY;
        released += keys.release();
        released += values.release();
        tracker.remove(released);

        keys = null;
        values = null;
        assigned = 0L;
        mask = 0L;
    }

    private void initialBuffers(long expectedElements) {
        allocateBuffers(minBufferSize(expectedElements), tracker);
    }

    /**
     * Convert the contents of this map to a human-friendly string.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append('[');

        HugeCursor<long[]> keys = this.keys.cursor(this.keys.newCursor());
        HugeCursor<double[]> values = this.values.cursor(this.values.newCursor());

        long key1, key2, slot;
        while (values.next()) {
            double[] vs = values.array;
            int vpos = values.offset;
            int vend = values.limit;

            int kpos = keys.offset;
            int kend = keys.limit;
            long[] ks = keys.array;

            while (vpos < vend) {
                if (kpos >= kend) {
                    keys.next();
                    kpos = keys.offset;
                    kend = keys.limit;
                    ks = keys.array;
                }
                if ((key1 = ks[kpos]) != 0L) {
                    buffer
                            .append('(')
                            .append(key1 - 1L)
                            .append(',')
                            .append(ks[kpos + 1] - 1L)
                            .append(")=>")
                            .append(vs[vpos])
                            .append(", ");
                }
                kpos += 2;
                ++vpos;
            }
        }

        if (buffer.length() > 1) {
            buffer.setLength(buffer.length() - 1);
            buffer.setCharAt(buffer.length() - 1, ']');
        } else {
            buffer.append(']');
        }

        return buffer.toString();
    }

    private long hashKey(long key1, long key2) {
        return BitMixer.mix64(key1 ^ key2 ^ (long) this.keyMixer);
    }

    /**
     * Allocate new internal buffers. This method attempts to allocate
     * and assign internal buffers atomically (either allocations succeed or not).
     */
    private void allocateBuffers(long arraySize, AllocationTracker tracker) {
        assert BitUtil.isPowerOfTwo(arraySize);

        // Compute new hash mixer candidate before expanding.
        final int newKeyMixer = RandomSeed.next();

        // Ensure no change is done if we hit an OOM.
        HugeLongArray prevKeys = this.keys;
        HugeDoubleArray prevValues = this.values;
        try {
            this.keys = HugeLongArray.newArray(arraySize << 1, tracker);
            this.values = HugeDoubleArray.newArray(arraySize, tracker);
            keysCursor = keys.newCursor();
        } catch (OutOfMemoryError e) {
            this.keys = prevKeys;
            this.values = prevValues;
            throw e;
        }

        this.resizeAt = expandAtCount(arraySize);
        this.keyMixer = newKeyMixer;
        this.mask = arraySize - 1L;
    }

    /**
     * Rehash from old buffers to new buffers.
     */
    private void rehash(
            HugeLongArray fromKeys,
            HugeDoubleArray fromValues) {
        assert fromKeys.size() == fromValues.size() << 1 &&
                BitUtil.isPowerOfTwo(fromValues.size());

        // Rehash all stored key/value pairs into the new buffers.
        final HugeLongArray newKeys = this.keys;
        final HugeDoubleArray newValues = this.values;
        final long mask = this.mask;

        HugeCursor<long[]> keys = fromKeys.cursor(fromKeys.newCursor());
        HugeCursor<double[]> values = fromValues.cursor(fromValues.newCursor());

        long key1, key2, slot;

        int vpos = 0;
        int vend = 0;
        int kpos = 0;
        int kend = 0;
        double[] vs = null;
        long[] ks;

        while (keys.next()) {
            if (vpos >= vend) {
                values.next();
                vpos = values.offset;
                vend = values.limit;
                vs = values.array;
            }

            kpos = keys.offset;
            kend = keys.limit - 1;
            ks = keys.array;

            for (; kpos < kend; kpos += 2, ++vpos) {
                if ((key1 = ks[kpos]) != 0L) {
                    key2 = ks[kpos + 1];
                    slot = hashKey(key1, key2) & mask;
                    slot = findSlot(key1, key2, slot);
                    slot = ~(1L + slot);
                    newKeys.set(slot << 1, key1);
                    newKeys.set((slot << 1) + 1, key2);
                    newValues.set(slot, vs[vpos]);
                }
            }
        }

//        while (values.next()) {
//            double[] vs = values.array;
//            int vpos = values.offset;
//            int vend = values.limit;
//
//            int kpos = keys.offset;
//            int kend = keys.limit;
//            long[] ks = keys.array;
//
//            while (vpos < vend) {
//                if (kpos >= kend) {
//                    keys.next();
//                    kpos = keys.offset;
//                    kend = keys.limit;
//                    ks = keys.array;
//                }
//                if ((key1 = ks[kpos]) != 0L) {
//                    key2 = ks[kpos + 1];
//                    slot = hashKey(key1, key2) & mask;
//                    slot = findSlot(key1, key2, slot);
//                    slot = ~(1L + slot);
//                    newKeys.set(slot << 1, key1);
//                    newKeys.set((slot << 1) + 1, key2);
//                    newValues.set(slot, vs[vpos]);
//                }
//                kpos += 2;
//                ++vpos;
//            }
//        }
    }


    /**
     * This method is invoked when there is a new key/ value pair to be inserted into
     * the buffers but there is not enough empty slots to do so.
     *
     * New buffers are allocated. If this succeeds, we know we can proceed
     * with rehashing so we assign the pending element to the previous buffer
     * and rehash all keys, substituting new buffers at the end.
     */
    private void allocateThenInsertThenRehash(long slot, long pendingKey1, long pendingKey2, double pendingValue) {
        assert assigned == resizeAt;

        // Try to allocate new buffers first. If we OOM, we leave in a consistent state.
        final HugeLongArray prevKeys = this.keys;
        final HugeDoubleArray prevValues = this.values;
        allocateBuffers(nextBufferSize(mask + 1), tracker);
        assert this.keys.size() > prevKeys.size();

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        prevKeys.set(slot << 1, pendingKey1);
        prevKeys.set((slot << 1) + 1, pendingKey2);
        prevValues.set(slot, pendingValue);

        // Rehash old keys, including the pending key.
        rehash(prevKeys, prevValues);

        long released = 0L;
        released += prevKeys.release();
        released += prevValues.release();
        tracker.remove(released);
    }


    private final static int MIN_HASH_ARRAY_LENGTH = 4;

    private static long minBufferSize(long elements) {
        if (elements < 0L) {
            throw new IllegalArgumentException(
                    "Number of elements must be >= 0: " + elements);
        }

        long length = (long) Math.ceil((double) elements / LOAD_FACTOR);
        if (length == elements) {
            length++;
        }
        length = Math.max(MIN_HASH_ARRAY_LENGTH, BitUtil.nextHighestPowerOfTwo(length));
        return length;
    }

    private static long nextBufferSize(long arraySize) {
        assert BitUtil.isPowerOfTwo(arraySize);
        return arraySize << 1;
    }

    private static long expandAtCount(long arraySize) {
        assert BitUtil.isPowerOfTwo(arraySize);
        return Math.min(arraySize, (long) Math.ceil(arraySize * LOAD_FACTOR));
    }

    private static final class RandomSeed {
        private static final RandomSeed INSTANCE = new RandomSeed();

        private static int next() {
            return INSTANCE.newSeed();
        }

        private final AtomicLong seed;

        private RandomSeed() {
            seed = new AtomicLong(Containers.randomSeed64());
        }

        private int newSeed() {
            return (int) BitMixer.mix64(seed.incrementAndGet());
        }
    }
}
