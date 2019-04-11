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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

public final class SingleHugeWeightMapBuilder {

    private final int pageShift;
    private final long pageMask;
    private final int pageSize;
    private HugeWeightMap.Page[] pages;

    public SingleHugeWeightMapBuilder(long nodeCount) {
        ImportSizing sizing = ImportSizing.of(4, nodeCount);
        int pageSize = sizing.pageSize();
        int numberOfPages = sizing.numberOfPages();
        this.pageSize = pageSize;
        this.pages = new HugeWeightMap.Page[numberOfPages];
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = (long) (pageSize - 1);
        int pageIndex = 0;
        for (; pageIndex < numberOfPages; pageIndex++) {
            int sizeOfPage = (int) Math.min((long) this.pageSize, nodeCount - (((long) pageIndex) << pageShift));
            if (sizeOfPage > 0) {
                HugeWeightMap.Page page = new HugeWeightMap.Page(sizeOfPage, AllocationTracker.EMPTY);
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
        HugeWeightMap.Page page = pages[pageIdx];
        page.put(localId, target, weight);
    }
}
