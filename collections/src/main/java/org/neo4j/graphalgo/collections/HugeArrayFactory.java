package org.neo4j.graphalgo.collections;

public final class HugeArrayFactory {

    public static HugeByteArray newByteArray(long size, AllocationTracker tracker) {
        return HugeByteArrayFactory.newArray(size, tracker);
    }

    public static HugeCharArray newCharArray(long size, AllocationTracker tracker) {
        return HugeCharArrayFactory.newArray(size, tracker);
    }

    public static HugeShortArray newShortArray(long size, AllocationTracker tracker) {
        return HugeShortArrayFactory.newArray(size, tracker);
    }

    public static HugeIntArray newIntArray(long size, AllocationTracker tracker) {
        return HugeIntArrayFactory.newArray(size, tracker);
    }

    public static HugeFloatArray newFloatArray(long size, AllocationTracker tracker) {
        return HugeFloatArrayFactory.newArray(size, tracker);
    }

    public static HugeLongArray newLongArray(long size, AllocationTracker tracker) {
        return HugeLongArrayFactory.newArray(size, tracker);
    }

    public static HugeDoubleArray newDoubleArray(long size, AllocationTracker tracker) {
        return HugeDoubleArrayFactory.newArray(size, tracker);
    }

    public static <T> HugeObjectArray<T> newObjectArray(long size, AllocationTracker tracker) {
        return HugeObjectArrayFactory.newArray(size, tracker);
    }

    private HugeArrayFactory() {
        throw new UnsupportedOperationException("No instances");
    }
}
