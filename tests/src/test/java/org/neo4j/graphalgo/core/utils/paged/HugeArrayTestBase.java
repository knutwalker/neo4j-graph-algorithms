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
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class HugeArrayTestBase<Array, Box, Huge extends HugeArray<Array, Box, Huge>> extends RandomizedTest {

    private static final int PS = HugeArrays.PAGE_SIZE;

    @Test
    public final void shouldSetAndGet() {
        testArray(10, array -> {
            int index = between(2, 8);
            int value = between(42, 1337);

            array.boxedSet(index, box(value));
            assertEquals(value, get(array, index));
        });
    }

    @Test
    public final void shouldSetAllValues() {
        int size = between(10, 20);
        testArray(size, array -> {
            array.boxedSetAll(i -> box(1 << i));
            for (int index = 0; index < size; index++) {
                assertEquals(1L << index, get(array, index));
            }
        });
    }

    @Test
    public final void shouldFillValues() {
        int size = between(10, 20);
        int value = between(42, 1337);
        testArray(size, array -> {
            array.boxedFill(box(value));
            for (int index = 0; index < size; index++) {
                assertEquals(value, get(array, index));
            }
        });
    }

    @Test
    public final void shouldCopyValues() {
        int size = between(10, 20);
        testArray(size, array -> {
            testArray(size, target -> {
                array.boxedSetAll(i1 -> box((int) i1 + 1));
                array.copyTo(target, size);
                for (int i = 0; i < size; i++) {
                    assertEquals(i + 1, get(target, i));
                }
            });
        });
    }

    @Test
    public final void shouldResetValuesNotCopied() {
        testArray(10, 20, (array, size) -> {
            testArray(30, 40, (target, sizeTarget) -> {
                array.boxedFill(box(42));
                target.boxedFill(box(1337));

                array.copyTo(target, size);

                for (int i = 0; i < size; i++) {
                    assertEquals(42L, get(target, i));
                }
                for (int i = size; i < sizeTarget; i++) {
                    assertEquals(0L, get(target, i));
                }
            });
        });
    }

    @Test
    public final void shouldOnlyCopyValuesThatAreADefined() {
        testArray(30, 40, (array, size) -> {
            testArray(10, 20, (target, sizeTarget) -> {
                array.boxedFill(box(42));
                array.copyTo(target, size);

                for (int i = 0; i < sizeTarget; i++) {
                    assertEquals(42L, get(target, i));
                }
            });
        });
    }

    @Test
    public final void shouldArrayCopy() {
        testArray(13, 42, (source, sizeSource) -> {
            testArray(13, 42, (target, sizeTarget) -> {
                testArrayCopy(source, sizeSource, target, sizeTarget);
            });
        });
    }

    @Test
    public final void shouldArrayLargePages() {
        int sizeSource = between(100_000, 200_000);
        int sizeTarget = between(100_000, 200_000);
        Huge source = pagedArray(sizeSource);
        Huge target = pagedArray(sizeTarget);

        testArrayCopy(source, sizeSource, target, sizeTarget);
    }

    private void testArrayCopy(
            Huge source,
            int sizeSource,
            Huge target,
            int sizeTarget) {
        int copySize = Math.max(sizeSource, sizeTarget);
        int expectedCopySize = Math.min(sizeTarget, Math.min(copySize, sizeSource));

        source.boxedFill(box(42));
        target.boxedFill(box(1337));

        source.copyTo(target, copySize);

        for (int i = 0; i < expectedCopySize; i++) {
            assertEquals(42L, get(target, i));
        }
        for (int i = expectedCopySize; i < sizeTarget; i++) {
            assertEquals(0L, get(target, i));
        }
    }

    @Test
    public final void shouldReportSize() {
        int size = between(10, 20);
        testArray(size, array -> {
            assertEquals(size, array.size());
        });
    }

    @Test
    public final void shouldFreeMemoryUsed() {
        int size = between(10, 20);
        long expected = bufferSize(size);
        testArray(size, array -> {
            long freed = array.release();
            assertThat(freed, anyOf(is(expected), is(expected + 24)));
        });
    }

    @Test
    public final void shouldHaveSinglePageCursor() {
        int size = between(100, 200);
        Huge array = singleArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.cursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        testCursorContent(size, cursor);

        assertFalse(cursor.next());
    }

    @Test
    public final void shouldHaveCursorForSinglePage() {
        int size = between(100, 200);
        Huge array = pagedArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.cursor(array.newCursor());

        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(size, cursor.limit);

        testCursorContent(size, cursor);

        assertFalse(cursor.next());
    }

    private void testCursorContent(int size, HugeCursor<Array> cursor) {
        if (cursor.array instanceof int[]) {
            int[] expected = new int[size];
            Arrays.fill(expected, 42);
            int[] actual = Arrays.copyOfRange((int[]) cursor.array, cursor.offset, cursor.limit);
            assertArrayEquals(expected, actual);
        }

        else if (cursor.array instanceof long[]) {
            long[] expected = new long[size];
            Arrays.fill(expected, 42L);
            long[] actual = Arrays.copyOfRange((long[]) cursor.array, cursor.offset, cursor.limit);
            assertArrayEquals(expected, actual);
        }

        else if (cursor.array instanceof double[]) {
            double[] expected = new double[size];
            Arrays.fill(expected, 42D);
            double[] actual = Arrays.copyOfRange((double[]) cursor.array, cursor.offset, cursor.limit);
            assertArrayEquals(expected, actual, 1e-4);
        }

        else if (cursor.array instanceof String[]) {
            String[] expected = (String[]) cursor.array;
            Arrays.fill(expected, "42");
            String[] actual = Arrays.copyOfRange((String[]) cursor.array, cursor.offset, cursor.limit);
            assertArrayEquals(expected, actual);
        }

        else fail("Unsupported type under test " + cursor.array.getClass());
    }

    @Test
    public final void shouldHaveCursorForMultiplePages() {
        int size = between(100_000, 200_000);
        Huge array = pagedArray(size);
        array.boxedFill(box(42));
        HugeCursor<Array> cursor = array.cursor(array.newCursor());

        // first page
        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(0, cursor.base);
        assertEquals(PS, cursor.limit);

        // middle pages
        int pageEnd = PS;
        while (pageEnd + PS < size) {
            assertTrue(cursor.next());
            assertEquals(0, cursor.offset);
            assertEquals(PS, cursor.limit);
            assertEquals(pageEnd, cursor.base);
            pageEnd += PS;
        }

        // last page
        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(pageEnd, cursor.base);
        assertEquals(size & (PS - 1), cursor.limit);

        assertFalse(cursor.next());
    }

    @Test
    public final void shouldHavePartialCursorForMultiplePages() {
        int size = between(100_000, 200_000);
        int start = between(10_000, 50_000);
        int end = between(start + 2 * PS, size);
        testPartialMultiCursor(size, start, end);
    }

    @Test
    public final void shouldHavePartialCursorForMultiplePagesWithFullPageSized() {
        testPartialMultiCursor(PS, 0, 0);
        testPartialMultiCursor(PS, 0, PS);
        testPartialMultiCursor(PS, PS, PS);

        testPartialMultiCursor(2 * PS, 0, 0);
        testPartialMultiCursor(2 * PS, 0, PS);
        testPartialMultiCursor(2 * PS, 0, 2 * PS);
        testPartialMultiCursor(2 * PS, PS, PS);
        testPartialMultiCursor(2 * PS, PS, 2 * PS);
        testPartialMultiCursor(2 * PS, 2 * PS, 2 * PS);

        testPartialMultiCursor(3 * PS, 0, 0);
        testPartialMultiCursor(3 * PS, 0, PS);
        testPartialMultiCursor(3 * PS, 0, 2 * PS);
        testPartialMultiCursor(3 * PS, 0, 3 * PS);
        testPartialMultiCursor(3 * PS, PS, PS);
        testPartialMultiCursor(3 * PS, PS, 2 * PS);
        testPartialMultiCursor(3 * PS, PS, 3 * PS);
        testPartialMultiCursor(3 * PS, 2 * PS, 2 * PS);
        testPartialMultiCursor(3 * PS, 2 * PS, 3 * PS);
        testPartialMultiCursor(3 * PS, 3 * PS, 3 * PS);
    }

    private void testPartialMultiCursor(int size, int start, int end) {
        Huge array = pagedArray(size);
        array.boxedSetAll(i1 -> box(42 + (int) i1));
        HugeCursor<Array> cursor = array.newCursor();
        array.cursor(cursor, start, end);

        long expected = start + 42L;
        while (cursor.next()) {
            for (int i = cursor.offset; i < cursor.limit; i++, expected++) {
                Box value = (Box) java.lang.reflect.Array.get(cursor.array, i);
                assertEquals(expected, unbox(value));
            }
        }
        assertEquals(expected, end + 42L);
    }

    @Test
    public final void shouldHaveCursor() {
        int size = between(100_000, 200_000);
        testArray(size, array -> {
            array.boxedSetAll(i1 -> box((int) i1 + 1));

            long actual = 0L;
            HugeCursor<Array> cursor = array.cursor(array.newCursor());
            while (cursor.next()) {
                int offset = cursor.offset;
                int limit = cursor.limit;
                for (int i = offset; i < limit; i++) {
                    Box value = (Box) java.lang.reflect.Array.get(cursor.array, i);
                    actual += unbox(value);
                }
            }

            long sum = ((long) size * (long) (size + 1)) / 2L;
            assertEquals(actual, sum);
        });
    }

    void testArray(int size, Consumer<Huge> block) {
        if (randomBoolean()) {
            block.accept(singleArray(size));
            block.accept(pagedArray(size));
        } else {
            block.accept(pagedArray(size));
            block.accept(singleArray(size));
        }
    }

    private void testArray(int sizeMin, int sizeMax, ObjIntConsumer<Huge> block) {
        int size;
        if (randomBoolean()) {
            size = between(sizeMin, sizeMax);
            block.accept(singleArray(size), size);
            size = between(sizeMin, sizeMax);
            block.accept(pagedArray(size), size);
        } else {
            size = between(sizeMin, sizeMax);
            block.accept(pagedArray(size), size);
            size = between(sizeMin, sizeMax);
            block.accept(singleArray(size), size);
        }
    }

    abstract Huge singleArray(int size);

    abstract Huge pagedArray(int size);

    abstract long bufferSize(int size);

    abstract Box box(int value);

    abstract int unbox(Box value);

    private int get(Huge array, int index) {
        return unbox(array.boxedGet((long) index));
    }
}
