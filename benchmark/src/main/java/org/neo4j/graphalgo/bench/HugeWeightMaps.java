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
package org.neo4j.graphalgo.bench;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

@State(Scope.Benchmark)
public class HugeWeightMaps {

    @Param({"10000", "100000", "1000000", "10000000", "100000000"})
    int size;

    long[] keys1;
    long[] keys2;
    double[] values;

    @Setup
    public void setup() {
        long[] keys1 = new long[size];
        long[] keys2 = new long[size];
        double[] values = new double[size];
        int[] distributions = new int[] {20, 10, 8, 5, 3, 2, 2, 2, 1, 1, 1, 1, 1, 1};
        Random rand = new Random(0);
        int nodes = size / 10;
        int relationships = size;
        int pos = 0;
        int source = 0;
        for (int distribution : distributions) {
            int degree = nodes * distribution / 100;
            degree = Math.min(degree, relationships);
            for (int j = 0; j < degree; j++) {
                int target = rand.nextInt(nodes);
                double value = rand.nextDouble();
                keys1[pos] = source;
                keys2[pos] = target;
                values[pos] = value;
                ++pos;
            }
            relationships -= degree;
            ++source;
        }
        for (; pos < size; ++pos, ++source) {
            long key1 = source % nodes;
            long key2 = rand.nextInt(nodes);
            double value = rand.nextDouble();
            keys1[pos] = key1;
            keys2[pos] = key2;
            values[pos] = value;
        }

        this.keys1 = keys1;
        this.keys2 = keys2;
        this.values = values;
    }
}
