package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.graphalgo.core.utils.PoolSizeKernelExtension;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;

import static org.junit.Assert.assertEquals;

public final class PoolSizeKernelExtensionTest {

    @Test
    public void limitConcurrencyOnCommunityEdition() {
        // just to make sure in case we run on an environment with less cores
        // set this to a value larger than the CE limitation
        System.setProperty("neo4j.graphalgo.processors", "8");
        PoolSizeKernelExtension extension = new PoolSizeKernelExtension();
        extension.newInstance(ctx(DatabaseInfo.COMMUNITY), deps());
        assertEquals(4, PoolSizeKernelExtension.concurrency());
    }

    @Test
    public void limitDefaultConcurrencyOnCommunityEdition() {
        PoolSizeKernelExtension extension = new PoolSizeKernelExtension();
        extension.newInstance(ctx(DatabaseInfo.COMMUNITY), deps());
        assertEquals(4, PoolSizeKernelExtension.concurrency(1337));
    }

    @Test
    public void unlimitedDefaultConcurrencyOnEnterpriseEdition() {
        // set fixed value that we will assert on
        System.setProperty("neo4j.graphalgo.processors", "42");
        PoolSizeKernelExtension extension = new PoolSizeKernelExtension();
        extension.newInstance(ctx(DatabaseInfo.ENTERPRISE), deps());
        assertEquals(42, PoolSizeKernelExtension.concurrency());
    }

    @Test
    public void unlimitedConcurrencyOnEnterpriseEdition() {
        PoolSizeKernelExtension extension = new PoolSizeKernelExtension();
        extension.newInstance(ctx(DatabaseInfo.ENTERPRISE), deps());
        assertEquals(42, PoolSizeKernelExtension.concurrency(42));
    }

    private PoolSizeKernelExtension.Dependencies deps() {
        return new PoolSizeKernelExtension.Dependencies() {
        };
    }

    private KernelContext ctx(DatabaseInfo info) {
        return new SimpleKernelContext(null, info, null);
    }

}
