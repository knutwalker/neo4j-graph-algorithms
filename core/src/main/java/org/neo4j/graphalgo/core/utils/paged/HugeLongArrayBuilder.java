package org.neo4j.graphalgo.core.utils.paged;

public final class HugeLongArrayBuilder extends HugeArrayBuilder<long[], HugeLongArray> {

    public static HugeLongArrayBuilder of(long numberOfNodes, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(numberOfNodes, tracker);
        return new HugeLongArrayBuilder(array, numberOfNodes);
    }

    private HugeLongArrayBuilder(HugeLongArray array, final long numberOfNodes) {
        super(array, numberOfNodes);
    }
}
