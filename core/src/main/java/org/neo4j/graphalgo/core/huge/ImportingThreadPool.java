package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.helpers.Exceptions;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class ImportingThreadPool {

    private static final long ONE_SECOND_IN_NANOS = TimeUnit.SECONDS.toNanos(1L);

    interface CanStop {
        boolean canStop();
    }

    interface Create<T> {
        T create(int index, CanStop canStop);
    }

    interface CreateScanner extends Create<RelationshipsScanner2> {
        void onScannerFinish(int numberOfRunningImporters);
    }

    interface CreateImporter extends Create<RelationshipImporter2> {
    }

    enum ComputationResult {
        OUT_OF_WORK, DONE, ABORT
    }

    private final int numberOfScannerThreads;
    private final CreateScanner createScanner;
    private final int numberOfImporterThreads;
    private final CreateImporter createImporter;

    ImportingThreadPool(
            final int numberOfScannerThreads,
            final CreateScanner createScanner,
            final int numberOfImporterThreads,
            final CreateImporter createImporter) {
        this.numberOfScannerThreads = numberOfScannerThreads;
        this.createScanner = createScanner;
        this.numberOfImporterThreads = numberOfImporterThreads;
        this.createImporter = createImporter;
    }

    long run(ExecutorService pool) {
        if (!ParallelUtil.canRunInParallel(pool)) {
            throw new IllegalStateException("Provided thread pool [" + pool + "] cannot run tasks.");
        }

        AtomicInteger scannerIndex = new AtomicInteger();
        AtomicInteger importerIndex = new AtomicInteger();
        AtomicInteger scannersActive = new AtomicInteger();
        AtomicInteger importersActive = new AtomicInteger();
        Collection<Throwable> failures = new ConcurrentLinkedDeque<>();
        Collection<RelationshipsScanner2> scanners = new ConcurrentLinkedDeque<>();
        Collection<RelationshipImporter2> importers = new ConcurrentLinkedDeque<>();
        Collection<Future<ComputationResult>> importerJobs = new ConcurrentLinkedDeque<>();
        Collection<Future<ComputationResult>> scannerJobs = new ConcurrentLinkedDeque<>();

        Supplier<RelationshipImporter2> createsImporter =
                creates(createImporter, importerIndex, importersActive);

        Supplier<RelationshipsScanner2> createsScanner =
                creates(createScanner, scannerIndex, scannersActive);

        Consumer<RelationshipImporter2> newImporter = importer -> {
            importers.add(importer);
            importersActive.incrementAndGet();
            importerJobs.add(pool.submit(importer));
        };

        Consumer<RelationshipsScanner2> newScanner = scanner -> {
            scanners.add(scanner);
            scannersActive.incrementAndGet();
            scannerJobs.add(pool.submit(scanner));
        };

        createThreads(createsImporter, newImporter, numberOfImporterThreads);

        long scannerStart = System.nanoTime();
        createThreads(createsScanner, newScanner, numberOfScannerThreads);

        int notYetDone;
        while (!scannerJobs.isEmpty()) {
            notYetDone = checkForNotYetDone(scannerJobs, failures, "scanner");
            maybeCreateThreads(createsImporter, importers, newImporter, notYetDone);
            sleepOnWorkingThreads(notYetDone);

            notYetDone = checkForNotYetDone(importerJobs, failures, "importer");
            maybeCreateThreads(createsScanner, scanners, newScanner, notYetDone);
            sleepOnWorkingThreads(notYetDone);
        }

        createScanner.onScannerFinish(numberOfImporterThreads);

        long scannerStop = System.nanoTime();

        checkForNotYetDone(importerJobs, failures, /* forceWait */ true, "importer");
        throwFailures(failures);

        return scannerStop - scannerStart;
    }

    private static <T> Supplier<T> creates(Create<T> create, AtomicInteger index, AtomicInteger active) {
        return () -> create.create(index.getAndIncrement(), () -> canStop(active));
    }

    private static boolean canStop1(AtomicInteger active) {
        int currentlyActive = active.get();
        while (currentlyActive > 1) {
            if (active.compareAndSet(currentlyActive, currentlyActive - 1)) {
                return true;
            }
            currentlyActive = active.get();
        }
        return false;
    }

    private static boolean canStop(AtomicInteger active) {
        int currentlyActive;
        do {
            currentlyActive = active.get();
        }
        while (currentlyActive > 1 && !active.compareAndSet(currentlyActive, currentlyActive - 1));
        return currentlyActive > 1;
    }

    private static <T> void maybeCreateThreads(
            Supplier<T> creator,
            Collection<T> items,
            Consumer<T> sideEffect,
            int numberToCreate) {
        if (numberToCreate > 0) {
            createThreads(creator, sideEffect.andThen(items::add), numberToCreate);
        }
    }

    private static <T> void createThreads(
            Supplier<T> creator,
            Consumer<T> sideEffect,
            int numberToCreate) {
        for (int i = 0; i < numberToCreate; i++) {
            sideEffect.accept(creator.get());
        }
    }

    private static int checkForNotYetDone(
        Collection<? extends Future<ComputationResult>> futures,
        Collection<? super Throwable> failures,
        String debugLabel) {
        return checkForNotYetDone(futures, failures, false, debugLabel);
    }

    private static int checkForNotYetDone(
            Collection<? extends Future<ComputationResult>> futures,
            Collection<? super Throwable> failures,
            boolean forceWait,
            String debugLabel) {
        int notYetDone = 0;
        int stillRunning = 0;
        Iterator<? extends Future<ComputationResult>> it = futures.iterator();
        while (it.hasNext()) {
            Future<ComputationResult> future = it.next();
            if (future.isDone() || forceWait) {
                try {
                    ComputationResult result = future.get();
                    if (result == ComputationResult.OUT_OF_WORK) {
                        notYetDone++;
                    }
                } catch (ExecutionException e) {
                    failures.add(e.getCause());
                } catch (InterruptedException ignored) {
                } finally {
                    it.remove();
                }
            } else {
                stillRunning++;
            }
        }
        if (notYetDone > 0) {
            System.out.println(debugLabel + ": notYetDone = " + notYetDone);
            return notYetDone;
        }
        return -stillRunning;
    }

    private static void sleepOnWorkingThreads(final int notYetDone) {
        if (notYetDone < 0) {
            LockSupport.parkNanos(ONE_SECOND_IN_NANOS);
        }
    }

    private static void throwFailures(Collection<? extends Throwable> failures) {
        Throwable error = null;
        for (Throwable cause : failures) {
            if (error != cause) {
                error = Exceptions.chain(error, cause);
            }
        }
        if (error != null) {
            throw ExceptionUtil.asUnchecked(error);
        }
    }
}
