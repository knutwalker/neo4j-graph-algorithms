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

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodeVLongs;
import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodedVLongSize;

final class AdjacencyCompression {

    static final int CHUNK_SIZE = 64;
    static final long[] EMPTY_LONGS = new long[0];

    static abstract class IntValue {
        int value;
    }

    private long[] ids;
    private int length;

    AdjacencyCompression() {
        this.ids = EMPTY_LONGS;
    }

    static long[] growWithDestroy(long[] values, int newLength) {
        if (values.length < newLength) {
            // give leeway in case of nodes with a reference to themselves
            // due to automatic skipping of identical targets, just adding one is enough to cover the
            // self-reference case, as it is handled as two relationships that aren't counted by BOTH
            // avoid repeated re-allocation for smaller degrees
            // avoid generous over-allocation for larger degrees
            int newSize = Math.max(32, 1 + newLength);
            return new long[newSize];
        }
        return values;
    }

    @Deprecated
    void copyFrom(CompressedLongArray array) {
        ids = growWithDestroy(ids, array.length());
        length = array.uncompress(ids);
    }

    static void copyFrom(LongsRef into, CompressedLongArray array) {
        into.longs = growWithDestroy(into.longs, array.length());
        into.length = array.uncompress(into.longs);
    }

    @Deprecated
    void applyDeltaEncoding() {
        final LongsRef data = new LongsRef(ids, 0, length);
        length = applyDeltaEncoding(data);
        ids = data.longs;
    }

    static int applyDeltaEncoding(LongsRef data) {
        Arrays.sort(data.longs, 0, data.length);
        return data.length = applyDelta(data.longs, data.length);
    }

    @Deprecated
    int writeDegree(byte[] out, int offset) {
        return writeDegree(out, offset, length);
    }

    @Deprecated
    int compress(byte[] out) {
        return encodeVLongs(ids, length, out);
    }

    static int compress(LongsRef data, byte[] out) {
        return encodeVLongs(data.longs, data.length, out);
    }

    @Deprecated
    void release() {
        ids = null;
        length = 0;
    }

    static long applyDeltaEncodingAndCalculateRequiredBytes(long[] ids, int length) {
        Arrays.sort(ids, 0, length);
        return applyDeltaAndCalculateRequiredBytes(ids, length);
    }

    static int compress(long[] ids, int length, byte[] out, int offset) {
        return encodeVLongs(ids, length, out, offset);
    }

    //@formatter:off
    static int writeDegree(byte[] out, int offset, int degree) {
        out[    offset] = (byte) (degree);
        out[1 + offset] = (byte) (degree >>> 8);
        out[2 + offset] = (byte) (degree >>> 16);
        out[3 + offset] = (byte) (degree >>> 24);
        return 4 + offset;
    }
    //@formatter:on

    private static long applyDeltaAndCalculateRequiredBytes(long values[], int length) {
        long value = values[0], delta;
        int in = 1, out = 1, bytes = encodedVLongSize(value);
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L) {
                bytes += encodedVLongSize(delta);
                values[out++] = delta;
            }
        }
        return RawValues.combineIntInt(out, bytes);
    }

    private static int applyDelta(long values[], int length) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L) {
                values[out++] = delta;
            }
        }
        return out;
    }
}
