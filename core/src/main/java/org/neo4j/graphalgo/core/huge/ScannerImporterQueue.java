package org.neo4j.graphalgo.core.huge;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpmcArrayQueue;
import org.neo4j.graphdb.Direction;

import java.util.Queue;

final class ScannerImporterQueue {

    private final Queue<RelationshipsBatch> transportQueue;
    private final Queue<RelationshipsBatch> batchPool;
    private final ScannerBuffer outBuffer;
    private final ScannerBuffer inBuffer;

    ScannerImporterQueue(
            int poolCapacity,
            Queue<RelationshipsBatch> queue,
            ScannerBuffer outBuffer,
            ScannerBuffer inBuffer) {
        this.transportQueue = queue;
        this.batchPool = newPool(poolCapacity);
        this.outBuffer = outBuffer;
        this.inBuffer = inBuffer;
    }

//    void batchRelationship(
//            ScannerBuffer buffer,
//            long source,
//            long target,
//            int baseFlags,
//            Direction direction) {
//        int len = buffer.addRelationship(source, target);
//        if (len >= batchSize) {
//            sendRelationship(source, buffer, baseFlags, direction);
//        }
//    }
//
//    void batchRelationshipWithProperties(
//            ScannerBuffer buffer,
//            long source,
//            long target,
//            RelationshipDataAccessor relData,
//            int baseFlags,
//            Direction direction) {
//        int len = buffer.addRelationshipWithProperties(
//                source,
//                target,
//                relData.relationshipReference(),
//                relData.propertiesReference());
//        if (len >= batchSize) {
//            sendRelationship(source, buffer, baseFlags, direction);
//        }
//    }

//    void sendRelationship(
//            long nodeId,
//            ScannerBuffer buffer,
//            int baseFlags,
//            Direction direction) {
//        RelationshipsBatch batch = nextRelationshipBatch(baseFlags, direction);
//        int length = buffer.swap(nodeId, batch);
//        if (length > 0) {
//            sendToImportThread(transportQueue, batch);
//        }
//    }

    long forceSendBatch(RelationshipsBatch batch) {
        return sendToImportThread(transportQueue, batch);
    }

    void drainOutBuffer(Direction outDirection, int outFlags) {
        drainAndRelease(outDirection, outFlags, outBuffer);
    }

    void drainInBuffer(Direction inDirection, int inFlags) {
        drainAndRelease(inDirection, inFlags, inBuffer);
    }

    void drainBuffer(
            Direction outDirection,
            int outFlags,
            Direction inDirection,
            int inFlags) {
        drainOutBuffer(outDirection, outFlags);
        drainInBuffer(inDirection, inFlags);
    }

    void drainBatchPool() {
        if (batchPool instanceof MessagePassingQueue) {
            //noinspection unchecked
            MessagePassingQueue<RelationshipsBatch> pool = (MessagePassingQueue<RelationshipsBatch>) batchPool;
            pool.drain(batch -> {
                batch.sourceTargetIds = null;
                batch.length = 0;
            });
        } else {
            RelationshipsBatch batch;
            while ((batch = batchPool.poll()) != null) {
                batch.sourceTargetIds = null;
                batch.length = 0;
            }
        }
    }

    void sendSentinels(int howMany) {
        for (int i = 0; i < howMany; i++) {
            sendToImportThread(transportQueue, RelationshipsBatch.SENTINEL);
        }
    }

    private void drainAndRelease(Direction direction, int flags, ScannerBuffer buffer) {
        if (buffer != null) {
            buffer.drainAndRelease((bucket, bucketLength) -> {
                RelationshipsBatch batch = nextRelationshipBatch(flags, direction);
                batch.setData(bucket, bucketLength, 0);
                sendToImportThread(transportQueue, batch);
            });
        }
    }

    RelationshipsBatch nextRelationshipBatch(
            int baseFlags,
            Direction direction) {
        RelationshipsBatch loaded = batchPool.poll();
        while (loaded == null) {
            // TODO: java 9 migration: Thread.onSpinWait()
            loaded = batchPool.poll();
        }
        loaded.setInfo(direction, baseFlags);
        return loaded;
    }

    private static long sendToImportThread(Queue<RelationshipsBatch> queue, RelationshipsBatch batch) {
        long retries = 0L;
        do {
            if (queue.offer(batch)) {
                return retries;
            }
            ++retries;
        } while (true);
    }

    private static Queue<RelationshipsBatch> newPool(int capacity) {
        final MpmcArrayQueue<RelationshipsBatch> pool = new MpmcArrayQueue<>(capacity);
        int i = pool.capacity();
        while (i-- > 0) {
            pool.add(new RelationshipsBatch(pool));
        }
        return pool;
    }

    // TODO: add the mpsc queue
    // TODO: add batch buffer from scanner
    // TODO: add the drain and release methods
    // TODO: add sentinel sending
}
