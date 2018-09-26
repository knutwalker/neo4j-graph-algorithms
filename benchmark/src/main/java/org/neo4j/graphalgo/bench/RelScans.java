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
package org.neo4j.graphalgo.bench;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphalgo.core.huge.PageCacheScanner;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.paged.AllocationTracker.humanReadable;

public final class RelScans {

    private static final MethodType STRING_TO_VOID = MethodType.methodType(void.class, String.class);
    private static final MethodType ACTION_TYPE = MethodType.methodType(
            long.class,
            GraphDatabaseAPI.class,
            RelationshipStore.class,
            int.class,
            long.class);
    private static final MethodType SCAN_ACTION_TYPE = MethodType.methodType(
            long.class,
            GraphDatabaseAPI.class,
            RelationshipStore.class,
            int.class,
            int.class,
            long.class);

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final boolean HAS_PROFILER;
    private static final MethodHandle ADD_BOOKMARK;


    static {
        boolean hasProfiler = false;
        MethodHandle addBookmark = MethodHandles.constant(Void.class, null);
        try {
            Class.forName("com.jprofiler.agent.ControllerImpl");
            Class<?> apiClass = Class.forName("com.jprofiler.api.agent.Controller");
            addBookmark = LOOKUP.findStatic(apiClass, "addBookmark", STRING_TO_VOID);
            hasProfiler = true;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ignored) {
        }
        HAS_PROFILER = hasProfiler;
        ADD_BOOKMARK = addBookmark;
    }


    public static void main(final String... args) throws Exception {
        main2(args);
    }

    private static void main2(final String... args) throws Exception {
        String jvmArgs = ManagementFactory
                .getRuntimeMXBean()
                .getInputArguments()
                .stream()
                .filter(s -> s.startsWith("-X"))
                .collect(Collectors.joining(" "));

        Log log = FormattedLog
                .withLogLevel(Level.DEBUG)
                .withZoneId(ZoneId.systemDefault())
                .withDateTimeFormatter(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .toOutputStream(System.out);
        log.info("Started with JVM args %s", jvmArgs);

        Collection<String> argv = new HashSet<>(Arrays.asList(args));
        boolean skipBarrier = argv.remove("-skipBarrier");

        AtomicReference<String> graphConfig = new AtomicReference<>("L10");
        Collection<Integer> multiScanPrefetchSizes = new ArrayList<>();
        argv.removeIf(arg -> {
            if (arg.startsWith("-db=")) {
                String dbToLoad = arg.split("=")[1].trim();
                graphConfig.set(dbToLoad);
                return true;
            }
            if (arg.startsWith("-prefetch=") || arg.startsWith("-preFetch=")) {
                String prefetchValues = arg.split("=")[1].trim();
                Arrays.stream(prefetchValues.split(","))
                        .map(String::trim)
                        .map(Integer::valueOf)
                        .forEach(multiScanPrefetchSizes::add);
                return true;
            }
            return false;
        });

        if (multiScanPrefetchSizes.isEmpty()) {
            multiScanPrefetchSizes.add(1000);
        }
        String graphToLoad = graphConfig.get();

        final Collection<String> methodNames = Arrays.asList(
                "singleThreadAllRecords",
                "singleThreadOnlyPages",
                "singleThreadScanAllRecords",
                "multiThreadedAllRecords",
                "multiThreadedOnlyPages",
                "multiThreadedScanAllRecords"
        );

        final Collection<Action> methodsToRun = methodNames
                .stream()
                .filter(matches(argv))
                .flatMap(m -> find(multiScanPrefetchSizes, m))
                .collect(Collectors.toList());

        final Collection<String> messages = new ArrayList<>(methodsToRun.size());

        if (!skipBarrier) {
            System.out.println("Press [Enter] to start");
            @SuppressWarnings("unused") int read = System.in.read();
            System.out.println("Starting...");
        }

        System.gc();

        try {
            for (Action methodAction : methodsToRun) {
                log.info("Running test: %s", methodAction);
                messages.add(doWork(graphToLoad, log, methodAction));
            }
        } catch (Throwable throwable) {
            throw ExceptionUtil.asUnchecked(throwable);
        }

        log.info("after scanning store");
        System.gc();

        jprofBookmark("shutdown");
        Pools.DEFAULT.shutdownNow();
        System.gc();

        for (String message : messages) {
            log.info(message);
        }
    }

    private static Predicate<String> matches(Collection<String> provided) {
        if (provided.isEmpty()) {
            return name -> true;
        }
        return name -> matches(provided, name.toLowerCase());
    }

    private static boolean matches(Collection<String> provided, String name) {
        for (String s : provided) {
            if (name.equalsIgnoreCase(s) || name.startsWith(s) || name.endsWith(s)) {
                return true;
            }
            s = s.toLowerCase();
            if (name.equals(s) || name.startsWith(s) || name.endsWith(s)) {
                return true;
            }
        }
        return false;
    }

    private static Stream<Action> find(Collection<Integer> prefetchSizes, String name) {
        try {
            return Stream.of(Action.of(LOOKUP.findStatic(RelScans.class, name, ACTION_TYPE)));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            try {
                MethodHandle handle = LOOKUP.findStatic(RelScans.class, name, SCAN_ACTION_TYPE);
                String baseLabel = nameFromHandle(handle);
                String baseToString = toStringFromHandle(handle);
                return prefetchSizes.stream()
                        .map(s -> {
                            MethodHandle newHandle = MethodHandles.insertArguments(handle, 2, s);
                            return new Action(
                                    newHandle,
                                    baseLabel + ", prefetch=" + s,
                                    baseToString
                            );
                        });
            } catch (NoSuchMethodException | IllegalAccessException e1) {
                throw new IllegalArgumentException("Could not find method '" + name + "' of form '" + ACTION_TYPE + "' in " + RelScans.class);
            }
        }
    }

    @SuppressWarnings("unused")
    private static long singleThreadAllRecords(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int recordsPerPage,
            long highestRecordId) {
        final RelationshipRecord record = relationshipStore.newRecord();
        long total;
        for (total = 0L; total < highestRecordId; ++total) {
            relationshipStore.getRecord(total, record, RecordLoad.CHECK);
        }
        return total;
    }

    @SuppressWarnings("unused")
    private static long singleThreadOnlyPages(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int recordsPerPage,
            long highestRecordId) {
        long total = 0L;
        final RelationshipRecord record = relationshipStore.newRecord();
        for (long i = 0L; i < highestRecordId; i += recordsPerPage) {
            relationshipStore.getRecord(i, record, RecordLoad.CHECK);
            ++total;
        }
        return total;
    }

    @SuppressWarnings("unused")
    private static long singleThreadScanAllRecords(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int prefetchSize,
            int recordsPerPage,
            long highestRecordId) {
        long total = 0L;
        PageCacheScanner scanner = new PageCacheScanner(db, prefetchSize, -1);
        try (PageCacheScanner.Cursor cursor = scanner.getCursor()) {
            while (cursor.next()) {
                ++total;
            }
        }
        return total;
    }

    @SuppressWarnings("unused")
    private static long multiThreadedAllRecords(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int recordsPerPage,
            long highestRecordId) {
        final RelationshipRecord record = relationshipStore.newRecord();
        return loadRecords(recordsPerPage, highestRecordId, loadAllRecords(recordsPerPage, highestRecordId, record, relationshipStore), db);
    }

    @SuppressWarnings("unused")
    private static long multiThreadedOnlyPages(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int recordsPerPage,
            long highestRecordId) {
        final RelationshipRecord record = relationshipStore.newRecord();
        return loadRecords(recordsPerPage, highestRecordId, loadRecords(record, relationshipStore), db);
    }

    @SuppressWarnings("unused")
    private static long multiThreadedScanAllRecords(
            GraphDatabaseAPI db,
            RelationshipStore relationshipStore,
            int prefetchSize,
            int recordsPerPage,
            long highestRecordId) {
        PageCacheScanner scanner = new PageCacheScanner(db, prefetchSize, -1);
        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(Pools.DEFAULT.submit(() -> {
                long total = 0L;
                try (PageCacheScanner.Cursor cursor = scanner.getCursor()) {
                    while (cursor.next()) {
                        ++total;
                    }
                }
                return total;
            }));
        }
        return removeDone(futures, true);
    }

    private static String doWork(String graphToLoad, Log log, Action action) throws Throwable {

        GraphDatabaseAPI db = LdbcDownloader.openDb(graphToLoad);
        DependencyResolver dep = db.getDependencyResolver();
        RelationshipStore relationshipStore = dep
                .resolveDependency(RecordStorageEngine.class)
                .testAccessNeoStores()
                .getRelationshipStore();

        String relStoreFileName = MetaDataStore.DEFAULT_NAME + StoreFactory.RELATIONSHIP_STORE_NAME;

        long relsInUse = 1L + relationshipStore.getHighestPossibleIdInUse();
        long recordsPerPage = (long) relationshipStore.getRecordsPerPage();
        long idsInPages = ((relsInUse + (recordsPerPage - 1L)) / recordsPerPage) * recordsPerPage;
        long requiredBytes = dep.resolveDependency(PageCache.class)
                .listExistingMappings()
                .stream()
                .map(PagedFile::file)
                .filter(f -> f.getName().equals(relStoreFileName))
                .mapToLong(File::length)
                .filter(size -> size >= 0L)
                .peek(fileSize -> log.info("RelStore size: %,d (%s)", fileSize, humanReadable(fileSize)))
                .findAny()
                .orElseGet(() -> {
                    long fileSize = idsInPages * (long) relationshipStore.getRecordSize();
                    log.info("RelStore size (estimated): %,d (%s)", fileSize, humanReadable(fileSize));
                    return fileSize;
                });

        String label = action.label();

        System.gc();

        jprofBookmark("starting " + label);

        long start = System.nanoTime();
        long relsImported = action.run(db, relationshipStore, (int) recordsPerPage, idsInPages);
        long stop = System.nanoTime();
        long tookNanos = stop - start;

        BigInteger bigNanos = BigInteger.valueOf(tookNanos);
        BigInteger aBillion = BigInteger.valueOf(1_000_000_000L);
        double tookInSeconds = new BigDecimal(bigNanos).divide(new BigDecimal(aBillion), RoundingMode.CEILING).doubleValue();
        long bytesPerSecond = aBillion.multiply(BigInteger.valueOf(requiredBytes)).divide(bigNanos).longValueExact();

        final String logMsg = String.format(
                "[%s] took %.3f s, overall %s/s (%,d bytes/s), imported %,d rels (%,.2f/s)",
                label,
                tookInSeconds,
                humanReadable(bytesPerSecond),
                bytesPerSecond,
                relsImported,
                (double) relsImported / tookInSeconds
        );

        jprofBookmark("end " + label);

        log.info("after " + label);
        System.gc();

        db.shutdown();
        return logMsg;
    }

    private static long loadRecords(
            int recordsPerPage,
            long highestRecordId,
            ToLongFunction<long[]> loader,
            GraphDatabaseAPI db) {
        long total = 0L;
        int batchSize = 100_000;
        long[] ids = new long[batchSize];
        int idx = 0;
        List<Future<Long>> futures = new ArrayList<>(100);
        for (long id = 0; id < highestRecordId; id += recordsPerPage) {
            ids[idx++] = id;
            if (idx == batchSize) {
                long[] submitted = ids.clone();
                idx = 0;
                futures.add(inTxFuture(db, loader, submitted));
            }
            total += removeDone(futures, false);
        }
        if (idx > 0) {
            long[] submitted = Arrays.copyOf(ids, idx);
            futures.add(inTxFuture(db, loader, submitted));
        }
        total += removeDone(futures, true);
        return total;
    }

    private static ToLongFunction<long[]> loadRecords(RelationshipRecord record, RelationshipStore recordStore) {
        return submitted -> loadRecords(record, recordStore, submitted);
    }

    private static long loadRecords(RelationshipRecord record, RelationshipStore recordStore, long[] submitted) {
        long total = 0L;
        for (long recordId : submitted) {
            record.setId(recordId);
            record.clear();
            try {
                recordStore.getRecord(recordId, record, RecordLoad.CHECK);
                ++total;
            } catch (Exception ignore) {
                // ignore
            }
        }
        return total;
    }

    private static ToLongFunction<long[]> loadAllRecords(
            int recordsPerPage,
            long highestRecordId,
            RelationshipRecord record,
            RelationshipStore recordStore) {
        return submitted -> loadAllRecords(recordsPerPage, highestRecordId, record, recordStore, submitted);
    }

    private static long loadAllRecords(
            int recordsPerPage,
            long highestRecordId,
            RelationshipRecord record,
            RelationshipStore recordStore,
            long[] submitted) {
        long total = 0L;
        if (submitted.length != 0) {
            long last = Math.min(submitted[submitted.length - 1] + recordsPerPage, highestRecordId);
            for (long i = submitted[0]; i < last; i++) {
                record.setId(i);
                record.clear();
                try {
                    recordStore.getRecord(i, record, RecordLoad.CHECK);
                    ++total;
                } catch (Exception ignore) {
                    // ignore
                }
            }
        }
        return total;
    }

    private static long removeDone(List<Future<Long>> futures, boolean wait) {
        if (wait || futures.size() > 25) {
            Iterator<Future<Long>> it = futures.iterator();
            long total = 0L;
            while (it.hasNext()) {
                Future<Long> future = it.next();
                if (wait || future.isDone()) {
                    try {
                        total += future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        // log.warn("Error during task execution", e);
                    }
                    it.remove();
                }
            }
            return total;
        }
        return 0L;
    }

    private static Future<Long> inTxFuture(GraphDatabaseService db, ToLongFunction<long[]> loader, long[] submitted) {
        try {
            return Pools.DEFAULT.submit(() -> {
                long imported;
                try (Transaction tx = db.beginTx()) {
                    imported = loader.applyAsLong(submitted);
                    tx.success();
                }
                return imported;
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }

    private static void jprofBookmark(final String start) {
        if (HAS_PROFILER) {
            try {
                ADD_BOOKMARK.invokeExact(start);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    private static String nameFromHandle(final MethodHandle handle) {
        String methodName = LOOKUP.revealDirect(handle).getName();
        return Arrays.stream(StringUtils.splitByCharacterTypeCamelCase(methodName))
                .filter(StringUtils::isNotBlank)
                .map(String::toLowerCase)
                .collect(Collectors.joining(" "));
    }

    private static String toStringFromHandle(final MethodHandle handle) {
        Method method = LOOKUP.revealDirect(handle).reflectAs(Method.class, LOOKUP);
        StringBuilder sb = new StringBuilder();
        sb
                .append(method.getReturnType().getTypeName())
                .append(' ')
                .append(method.getName())
                .append('(');
        Class<?>[] types = method.getParameterTypes();
        for (Class<?> type : types) {
            sb.append(type.getTypeName()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        return sb.toString();
    }

    static final class Action {
        private final MethodHandle handle;
        private final String label;
        private final String toString;

        private Action(
                final MethodHandle handle,
                final String label,
                final String toString) {
            this.handle = handle;
            this.label = label;
            this.toString = toString;
        }

        static Action of(final MethodHandle handle) {
            return new Action(handle, nameFromHandle(handle), toStringFromHandle(handle));
        }

        long run(
                GraphDatabaseAPI db,
                RelationshipStore relationshipStore,
                int recordsPerPage,
                long highestRecordId) throws Throwable {
            return (long) handle.invokeExact(db, relationshipStore, recordsPerPage, highestRecordId);
        }

        String label() {
            return label;
        }

        @Override
        public String toString() {
            return toString;
        }

    }
}
