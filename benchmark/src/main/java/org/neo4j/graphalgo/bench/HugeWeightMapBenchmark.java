package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.huge.loader.SingleHugeWeightMapBuilder;
import org.neo4j.graphalgo.core.huge.loader.SingleOldHugeWeightMapBuilder;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongDoubleMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class HugeWeightMapBenchmark {

    @Benchmark
    public HugeWeightMapping hugeWeightMap(HugeWeightMaps data) {
        final long[] keys1 = data.keys1;
        final long[] keys2 = data.keys2;
        final double[] values = data.values;
        int len = keys1.length;
        SingleHugeWeightMapBuilder builder = new SingleHugeWeightMapBuilder(len);
        for (int i = 0; i < len; i++) {
            builder.add(keys1[i], keys2[2], values[i]);
            builder.add(keys2[i], keys1[2], values[i]);
        }
        return builder.build();
    }

    @Benchmark
    public HugeWeightMapping oldHugeWeightMap(HugeWeightMaps data) {
        final long[] keys1 = data.keys1;
        final long[] keys2 = data.keys2;
        final double[] values = data.values;
        int len = keys1.length;
        SingleOldHugeWeightMapBuilder builder = new SingleOldHugeWeightMapBuilder(len);
        for (int i = 0; i < len; i++) {
            builder.add(keys1[i], keys2[2], values[i]);
            builder.add(keys2[i], keys1[2], values[i]);
        }
        return builder.build();
    }

    @Benchmark
    public HugeLongLongDoubleMap longLongDoubleMap(HugeWeightMaps data) {
        final long[] keys1 = data.keys1;
        final long[] keys2 = data.keys2;
        final double[] values = data.values;
        int len = keys1.length;
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap(len, AllocationTracker.EMPTY);
        for (int i = 0; i < len; i++) {
            map.addTo(keys1[i], keys2[2], values[i]);
            map.addTo(keys2[i], keys1[2], values[i]);
        }
        return map;
    }

    @Benchmark
    public OldHugeLongLongDoubleMap oldLongLongDoubleMap(HugeWeightMaps data) {
        final long[] keys1 = data.keys1;
        final long[] keys2 = data.keys2;
        final double[] values = data.values;
        int len = keys1.length;
        OldHugeLongLongDoubleMap map = new OldHugeLongLongDoubleMap(len, AllocationTracker.EMPTY);
        for (int i = 0; i < len; i++) {
            map.addTo(keys1[i], keys2[2], values[i]);
            map.addTo(keys2[i], keys1[2], values[i]);
        }
        return map;
    }
}
