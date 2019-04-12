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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.container.TrackingLongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

public final class SingleOldHugeWeightMapBuilder {

    private final int pageShift;
    private final long pageMask;
    private final int pageSize;
    private OldPage[] pages;

    public SingleOldHugeWeightMapBuilder(long nodeCount) {
        ImportSizing sizing = ImportSizing.of(4, nodeCount);
        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();
        this.pageSize = pageSize;
        this.pages = new OldPage[numberOfPages];
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = (long) (pageSize - 1);
        int pageIndex = 0;
        for (; pageIndex < numberOfPages; pageIndex++) {
            int sizeOfPage = (int) Math.min((long) this.pageSize, nodeCount - (((long) pageIndex) << pageShift));
            if (sizeOfPage > 0) {
                OldPage page = new OldPage(sizeOfPage, AllocationTracker.EMPTY);
                pages[pageIndex] = page;
            } else {
                break;
            }
        }
        if (pageIndex < pages.length) {
            pages = Arrays.copyOf(pages, pageIndex);
        }
    }

    public HugeWeightMapping build() {
        return HugeWeightMap.of(pages, pageSize, 1.0, AllocationTracker.EMPTY);
    }

    public void add(long source, long target, double weight) {
        int pageIdx = (int) (source >>> pageShift);
        int localId = (int) (source & pageMask);
        OldPage page = pages[pageIdx];
        page.put(localId, target, weight);
    }

    static final class OldPage implements HugeWeightMapping {
        private static final long CLASS_MEMORY = shallowSizeOfInstance(HugeWeightMap.Page.class);

        private TrackingLongDoubleHashMap[] data;
        private final AllocationTracker tracker;
        private double defaultValue;

        OldPage(int pageSize, AllocationTracker tracker) {
            this.data = new TrackingLongDoubleHashMap[pageSize];
            this.tracker = tracker;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pageSize));
        }

        @Override
        public double weight(final long source, final long target) {
            return weight(source, target, defaultValue);
        }

        @Override
        public double weight(final long source, final long target, final double defaultValue) {
            int localIndex = (int) source;
            return get(localIndex, target, defaultValue);
        }

        double get(int localIndex, long target, double defaultValue) {
            TrackingLongDoubleHashMap map = data[localIndex];
            return map != null ? map.getOrDefault(target, defaultValue) : defaultValue;
        }

        void put(int localIndex, long target, double value) {
            mapForIndex(localIndex).put(target, value);
        }

        @Override
        public long release() {
            if (data != null) {
                long released = CLASS_MEMORY + sizeOfObjectArray(data.length);
                for (TrackingLongDoubleHashMap map : data) {
                    if (map != null) {
                        released += map.free();
                    }
                }
                data = null;
                return released;
            }
            return 0L;
        }

        private void setDefaultValue(double defaultValue) {
            this.defaultValue = defaultValue;
        }


        private TrackingLongDoubleHashMap mapForIndex(int localIndex) {
            TrackingLongDoubleHashMap map = data[localIndex];
            if (map == null) {
                map = data[localIndex] = new TrackingLongDoubleHashMap(tracker);
            }
            return map;
        }
    }
}
