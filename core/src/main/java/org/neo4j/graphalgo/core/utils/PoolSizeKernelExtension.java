package org.neo4j.graphalgo.core.utils;

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;


public final class PoolSizeKernelExtension extends KernelExtensionFactory<PoolSizeKernelExtension.Dependencies> {

    private static final String SERVICE_NAME = "GRAPH_ALGOS_POOL_SIZE";

    private static int DEFAULT_CONCURRENCY = 0;
    private static int MAX_CONCURRENCY = 0;

    public static int concurrency() {
        return DEFAULT_CONCURRENCY;
    }

    public static int concurrency(int targetConcurrency) {
        return Math.min(MAX_CONCURRENCY, targetConcurrency);
    }

    public PoolSizeKernelExtension() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) {
        boolean isOnEnterprise = context.databaseInfo().edition == Edition.enterprise;
        ConcurrencyConfig config = new ConcurrencyConfig(isOnEnterprise);
        MAX_CONCURRENCY = config.maxConcurrency;
        DEFAULT_CONCURRENCY = config.defaultConcurrency;
        return new LifecycleAdapter();
    }

    public interface Dependencies {
    }
}
