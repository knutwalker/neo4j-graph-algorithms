/**
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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

final class UnsafeArrayCas {

    static {
        UnsafeUtil.assertHasUnsafe();
    }

    private static final long INTV_BASE = arrayBaseOffset(int[].class);
    private static final int INTV_SHIFT = arrayIndexShift(int[].class);
    private static final long LONGV_BASE = arrayBaseOffset(long[].class);
    private static final int LONGV_SHIFT = arrayIndexShift(long[].class);
    private static final long REFV_BASE = arrayBaseOffset(Object[].class);
    private static final int REFV_SHIFT = arrayIndexShift(Object[].class);

    private static long arrayBaseOffset(Class<?> arrayClass) {
        return (long) UnsafeUtil.arrayBaseOffset(arrayClass);
    }

    private static final Object TOMBSTONE = new Tombstone();

    private static int arrayIndexShift(Class<?> arrayClass) {
        int scale = UnsafeUtil.arrayIndexScale(arrayClass);
        if ((scale & (scale - 1)) != 0) {
            throw new Error("data type scale not a power of two");
        }
        return 31 - Integer.numberOfLeadingZeros(scale);
    }

    static long getCheckedIntOffset(int[] array, int index) {
        return checkedIntByteOffset(array, index);
    }

    static void increment(long[] array, int index) {
        long offset = checkedLongByteOffset(array, index);
        UnsafeUtil.getAndAddLong(array, offset, 1L);
    }

    static int increment(int[] array, int index, int delta) {
        return uncheckedIncrement(array, checkedIntByteOffset(array, index), delta);
    }

    static int uncheckedIncrement(int[] array, long offset, int delta) {
        return UnsafeUtil.getAndAddInt(array, offset, delta);
    }

    static int get(int[] array, int index) {
        return UnsafeUtil.getIntVolatile(array, checkedIntByteOffset(array, index));
    }

    static void set(int[] array, int index, int value) {
        UnsafeUtil.putIntVolatile(array, checkedIntByteOffset(array, index), value);
    }

    static void uncheckedSet(int[] array, long offset, int value) {
        UnsafeUtil.putIntVolatile(array, offset, value);
    }

    static int getAndSet(int[] array, int index, int value) {
        // FIXME: add getAndSetInt to UnsafeUtil ?
        long offset = checkedIntByteOffset(array, index);
        int oldValue = UnsafeUtil.getIntVolatile(array, offset);
        UnsafeUtil.putIntVolatile(array, offset, value);
        return oldValue;
    }

    static long get(long[] array, int index) {
        return UnsafeUtil.getLongVolatile(array, checkedLongByteOffset(array, index));
    }

    @SuppressWarnings("unchecked")
    static <T> T get(T[] array, int index) {
        return (T) UnsafeUtil.getObjectVolatile(array, checkedRefByteOffset(array, index));
    }

    static <T> boolean cas(T[] array, int index, T expected, T update) {
        long offset = checkedRefByteOffset(array, index);
        return UnsafeUtil.compareAndSwapObject(array, offset, expected, update);
    }

    static <T> long getCheckedRefOffset(T[] array, int index) {
        return checkedRefByteOffset(array, index);
    }

    @SuppressWarnings("unchecked")
    static <T> T forceAcquireObject(T[] array, int index) {
        long offset = checkedRefByteOffset(array, index);
        return uncheckedForceAcquireObject(array, offset);
    }

    @SuppressWarnings("unchecked")
    static <T> T forceGetAndDeleteObject(T[] array, int index) {
        long offset = checkedRefByteOffset(array, index);
        return (T) uncheckedForceGetAndSetObject2(array, offset, null);
    }

    @SuppressWarnings("unchecked")
    static <T> T uncheckedForceAcquireObject(T[] array, long offset) {
        return (T) uncheckedForceGetAndSetObject2(array, offset, TOMBSTONE);
    }

    private static Object uncheckedForceGetAndSetObject(Object[] array, long offset, Object newValue) {
        int reps = 0;
        Object value;
        do {
            value = UnsafeUtil.getObject(array, offset);
            ++reps;
        }
        while (reps < 100 && (value == TOMBSTONE || !UnsafeUtil.compareAndSwapObject(array, offset, value, newValue)));
        if (value == TOMBSTONE) {
            long index = (offset - REFV_BASE) >> REFV_SHIFT;
            throw new IllegalStateException("couldn't get value at index " + index + " after " + reps + " tries: offset = " + offset + " new val " + newValue);
        }
        return value;
    }

    private static Object uncheckedForceGetAndSetObject2(Object[] array, long offset, Object newValue) {
        Object value;
        int reps = 0;
        int failedCas = 0;
        while (true) {
            value = UnsafeUtil.getObjectVolatile(array, offset);
            while (value == TOMBSTONE) {
                if (++reps >= 10_000) {
                    long index = (offset - REFV_BASE) >> REFV_SHIFT;
                    throw new IllegalStateException("cas error: tries=" + reps + " casFailures=" + failedCas + " index=" + index + " offset=" + offset);
                }
//                LockSupport.parkNanos(1000L);
                Thread.yield();
                value = UnsafeUtil.getObjectVolatile(array, offset);
            }
            if (UnsafeUtil.compareAndSwapObject(array, offset, value, newValue)) {
                return value;
            }
            ++failedCas;
        }
    }

    @SuppressWarnings("unchecked")
    static <T> void returnObject(T[] array, int index, T value) {
        long offset = checkedRefByteOffset(array, index);
        uncheckedReturnObject(array, offset, value);
    }

    @SuppressWarnings("unchecked")
    static <T> void uncheckedReturnObject(T[] array, long offset, T value) {
        if (value == TOMBSTONE) {
            long index = (offset - REFV_BASE) >> REFV_SHIFT;
            throw new IllegalStateException("unexpected TOMBSTONE: index=" + index + " offset=" + offset +  " old value (maybe outdated)=" + UnsafeUtil.getObjectVolatile(array, offset));
        }
        if (!UnsafeUtil.compareAndSwapObject(array, offset, TOMBSTONE, value)) {
            throw new IllegalStateException("returning an object which is not missing [" + value + "] + old value (maybe outdated) = " + UnsafeUtil.getObjectVolatile(array, offset));
        }
    }

    static boolean isTombstone(Object value) {
        return value == TOMBSTONE;
    }

    private static long checkedIntByteOffset(int[] array, int index) {
        return intByteOffset(checkIndex(index, array.length));
    }

    private static long checkedLongByteOffset(long[] array, int index) {
        return longByteOffset(checkIndex(index, array.length));
    }

    private static long checkedRefByteOffset(Object[] array, int index) {
        return refByteOffset(checkIndex(index, array.length));
    }

    private static long intByteOffset(int index) {
        return INTV_BASE + (((long) index) << INTV_SHIFT);
    }

    private static long longByteOffset(int index) {
        return LONGV_BASE + (((long) index) << LONGV_SHIFT);
    }

    private static long refByteOffset(int index) {
        return REFV_BASE + (((long) index) << REFV_SHIFT);
    }

    // migration path for Java >= 9: Objects::checkIndex(index, length)
    private static int checkIndex(int index, int length) {
        if (index < 0 || index >= length) {
            throw new ArrayIndexOutOfBoundsException("index " + index + " but length " + length);
        }
        return index;
    }

    private UnsafeArrayCas() {
        throw new UnsupportedOperationException("No instances");
    }

    private static final class Tombstone {
        @Override
        public String toString() {
            return "Tombstone";
        }
    }
}
