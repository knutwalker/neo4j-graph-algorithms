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

import org.junit.Test;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.paged.BitUtil.*;

public final class BitUtilTest {

    @Test
    public void shouldDetectPowersOfTwoForInts() {
        assertTrue(isPowerOfTwo(1));
        assertTrue(isPowerOfTwo(2));
        assertTrue(isPowerOfTwo(4));
        assertTrue(isPowerOfTwo(32));
        assertTrue(isPowerOfTwo(1 << 30));
        // value must be strictly positive
        assertFalse(isPowerOfTwo(Integer.MIN_VALUE));
        assertFalse(isPowerOfTwo(0));
        assertFalse(isPowerOfTwo(Integer.MIN_VALUE + 1));
        assertFalse(isPowerOfTwo(-1));
        assertFalse(isPowerOfTwo(3));
        assertFalse(isPowerOfTwo(31));
        assertFalse(isPowerOfTwo((1 << 30) + 1));
        assertFalse(isPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void shouldDetectPowersOfTwoForLongs() {
        assertTrue(isPowerOfTwo(1L));
        assertTrue(isPowerOfTwo(2L));
        assertTrue(isPowerOfTwo(4L));
        assertTrue(isPowerOfTwo(32L));
        assertTrue(isPowerOfTwo(1L << 30));
        assertTrue(isPowerOfTwo(1L << 31));
        assertTrue(isPowerOfTwo(1L << 32));
        // value must be strictly positive
        assertFalse(isPowerOfTwo(Long.MIN_VALUE));
        assertFalse(isPowerOfTwo(0L));
        assertFalse(isPowerOfTwo(Long.MIN_VALUE + 1));
        assertFalse(isPowerOfTwo(-1L));
        assertFalse(isPowerOfTwo(3L));
        assertFalse(isPowerOfTwo(31L));
        assertFalse(isPowerOfTwo((1L << 30L) + 1));
        assertFalse(isPowerOfTwo((1L << 31L) + 1));
        assertFalse(isPowerOfTwo((1L << 32L) + 1));
        assertFalse(isPowerOfTwo(Long.MAX_VALUE));
    }

    @Test
    public void shouldReturnNextPowerOfTwoForInts() {
        assertEquals(Integer.MIN_VALUE, nextHighestPowerOfTwo(Integer.MIN_VALUE));
        assertEquals(0, nextHighestPowerOfTwo(Integer.MIN_VALUE + 1));
        assertEquals(0, nextHighestPowerOfTwo(-1));
        assertEquals(0, nextHighestPowerOfTwo(0));
        assertEquals(1, nextHighestPowerOfTwo(1));
        assertEquals(2, nextHighestPowerOfTwo(2));
        assertEquals(4, nextHighestPowerOfTwo(3));
        assertEquals(4, nextHighestPowerOfTwo(4));
        assertEquals(32, nextHighestPowerOfTwo(31));
        assertEquals(32, nextHighestPowerOfTwo(32));
        assertEquals(1 << 30, nextHighestPowerOfTwo(1 << 30));
        assertEquals(Integer.MIN_VALUE, nextHighestPowerOfTwo((1 << 30) + 1));
        assertEquals(Integer.MIN_VALUE, nextHighestPowerOfTwo(Integer.MAX_VALUE - 1));
        assertEquals(Integer.MIN_VALUE, nextHighestPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void shouldReturnNextPowerOfTwoForLongs() {
        assertEquals(Long.MIN_VALUE, nextHighestPowerOfTwo(Long.MIN_VALUE));
        assertEquals(0L, nextHighestPowerOfTwo(Long.MIN_VALUE + 1L));
        assertEquals(0L, nextHighestPowerOfTwo(-1L));
        assertEquals(0L, nextHighestPowerOfTwo(0L));
        assertEquals(2L, nextHighestPowerOfTwo(2L));
        assertEquals(4L, nextHighestPowerOfTwo(3L));
        assertEquals(1L, nextHighestPowerOfTwo(1L));
        assertEquals(4L, nextHighestPowerOfTwo(4L));
        assertEquals(32L, nextHighestPowerOfTwo(31L));
        assertEquals(32L, nextHighestPowerOfTwo(32L));
        assertEquals(1L << 62, nextHighestPowerOfTwo(1L << 62));
        assertEquals(Long.MIN_VALUE, nextHighestPowerOfTwo((1L << 62) + 1));
        assertEquals(Long.MIN_VALUE, nextHighestPowerOfTwo(Long.MAX_VALUE - 1));
        assertEquals(Long.MIN_VALUE, nextHighestPowerOfTwo(Long.MAX_VALUE));
    }

    @Test
    public void shouldReturnPreviousPowerOfTwoForInts() {
        assertEquals(Integer.MIN_VALUE, previousPowerOfTwo(Integer.MIN_VALUE));
        assertEquals(Integer.MIN_VALUE, previousPowerOfTwo(Integer.MIN_VALUE + 1));
        assertEquals(Integer.MIN_VALUE, previousPowerOfTwo(-1));
        assertEquals(0, previousPowerOfTwo(0));
        assertEquals(1, previousPowerOfTwo(1));
        assertEquals(2, previousPowerOfTwo(2));
        assertEquals(2, previousPowerOfTwo(3));
        assertEquals(4, previousPowerOfTwo(4));
        assertEquals(16, previousPowerOfTwo(31));
        assertEquals(32, previousPowerOfTwo(32));
        assertEquals(1 << 30, previousPowerOfTwo(1 << 30));
        assertEquals(1 << 30, previousPowerOfTwo((1 << 30) + 1));
        assertEquals(1 << 30, previousPowerOfTwo(Integer.MAX_VALUE - 1));
        assertEquals(1 << 30, previousPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void shouldReturnPreviousPowerOfTwoForLongs() {
        assertEquals(Long.MIN_VALUE, previousPowerOfTwo(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, previousPowerOfTwo(Long.MIN_VALUE + 1));
        assertEquals(Long.MIN_VALUE, previousPowerOfTwo(-1L));
        assertEquals(0L, previousPowerOfTwo(0L));
        assertEquals(1L, previousPowerOfTwo(1L));
        assertEquals(2L, previousPowerOfTwo(2L));
        assertEquals(2L, previousPowerOfTwo(3L));
        assertEquals(4L, previousPowerOfTwo(4L));
        assertEquals(32L, previousPowerOfTwo(32));
        assertEquals(16L, previousPowerOfTwo(31L));
        assertEquals(1L << 62, previousPowerOfTwo(1L << 62));
        assertEquals(1L << 62, previousPowerOfTwo((1L << 62) + 1));
        assertEquals(1L << 62, previousPowerOfTwo(Long.MAX_VALUE - 1));
        assertEquals(1L << 62, previousPowerOfTwo(Long.MAX_VALUE));
    }

    @Test
    public void shouldReturnNearbyPowerOfTwoForInts() {
        assertEquals(Integer.MIN_VALUE, nearbyPowerOfTwo(Integer.MIN_VALUE));
        assertEquals(0, nearbyPowerOfTwo(Integer.MIN_VALUE + 1));
        assertEquals(0, nearbyPowerOfTwo(-1));
        assertEquals(0, nearbyPowerOfTwo(0));
        assertEquals(1, nearbyPowerOfTwo(1));
        assertEquals(2, nearbyPowerOfTwo(2));
        assertEquals(4, nearbyPowerOfTwo(3));
        assertEquals(4, nearbyPowerOfTwo(4));
        assertEquals(4, nearbyPowerOfTwo(5));
        assertEquals(8, nearbyPowerOfTwo(6));
        assertEquals(8, nearbyPowerOfTwo(7));
        assertEquals(8, nearbyPowerOfTwo(8));
        assertEquals(32, nearbyPowerOfTwo(31));
        assertEquals(32, nearbyPowerOfTwo(32));
        assertEquals(32, nearbyPowerOfTwo(33));
        assertEquals(1 << 30, nearbyPowerOfTwo(1 << 30));
        assertEquals(1 << 30, nearbyPowerOfTwo((1 << 30) + 1));
        assertEquals(Integer.MIN_VALUE, nearbyPowerOfTwo(Integer.MAX_VALUE - 1));
        assertEquals(Integer.MIN_VALUE, nearbyPowerOfTwo(Integer.MAX_VALUE));
    }

    @Test
    public void shouldReturnNearbyPowerOfTwoForLongs() {
        assertEquals(Long.MIN_VALUE, nearbyPowerOfTwo(Long.MIN_VALUE));
        assertEquals(0L, nearbyPowerOfTwo(Long.MIN_VALUE + 1));
        assertEquals(0L, nearbyPowerOfTwo(-1L));
        assertEquals(0L, nearbyPowerOfTwo(0L));
        assertEquals(1L, nearbyPowerOfTwo(1L));
        assertEquals(2L, nearbyPowerOfTwo(2L));
        assertEquals(4L, nearbyPowerOfTwo(3L));
        assertEquals(4L, nearbyPowerOfTwo(4L));
        assertEquals(4L, nearbyPowerOfTwo(5L));
        assertEquals(8L, nearbyPowerOfTwo(6L));
        assertEquals(8L, nearbyPowerOfTwo(7L));
        assertEquals(8L, nearbyPowerOfTwo(8L));
        assertEquals(32L, nearbyPowerOfTwo(31L));
        assertEquals(32L, nearbyPowerOfTwo(32L));
        assertEquals(32L, nearbyPowerOfTwo(33L));
        assertEquals(1L << 62, nearbyPowerOfTwo(1L << 62));
        assertEquals(1L << 62, nearbyPowerOfTwo((1L << 62) + 1));
        assertEquals(Long.MIN_VALUE, nearbyPowerOfTwo(Long.MAX_VALUE - 1));
        assertEquals(Long.MIN_VALUE, nearbyPowerOfTwo(Long.MAX_VALUE));
    }

    @Test
    public void shouldAlignToNextMultipleOfALignment() {
        assertEquals(0L, align(0L, 4));
        assertEquals(4L, align(1L, 4));
        assertEquals(4L, align(3L, 4));
        assertEquals(4L, align(4L, 4));
        assertEquals(8L, align(5L, 4));
        assertEquals(0L, align(-1L, 4));
        assertEquals(0L, align(-3L, 4));
        assertEquals(-4L, align(-4L, 4));
        assertEquals(-4L, align(-5L, 4));
        assertEquals(Long.MAX_VALUE - 3, align(Long.MAX_VALUE - 3, 4));
        assertEquals(Long.MIN_VALUE, align(Long.MAX_VALUE, 4));
    }
}
