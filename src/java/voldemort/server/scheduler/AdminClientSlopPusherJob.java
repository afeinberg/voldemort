package voldemort.server.scheduler;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

public class AdminClientSlopPusherJob {

    private final static Logger logger = Logger.getLogger(AdminClientSlopPusherJob.class.getName());

    private final static Pair<ByteArray, Versioned<Slop>> END = Pair.create(null, null);

    private final Cluster cluster;
    private final FailureDetector failureDetector;
    private final StoreRepository storeRepo;
    private final Map<Integer, SynchronousQueue<Pair<ByteArray, Versioned<Slop>>>> slopQueues;
    private final ExecutorService consumerExecutor;
    private final EventThrottler writeThrottler;
    private final EventThrottler readThrottler;
    private final AdminClient adminClient;

    public AdminClientSlopPusherJob(Cluster cluster,
                                    FailureDetector failureDetector,
                                    StoreRepository storeRepo,
                                    long maxReadBytesPerSec,
                                    long maxWriteBytesPerSec) {
        this.cluster = cluster;
        this.failureDetector = failureDetector;
        this.storeRepo = storeRepo;
        this.slopQueues = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        this.consumerExecutor = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        this.writeThrottler = new EventThrottler(maxWriteBytesPerSec);
        this.readThrottler = new EventThrottler(maxReadBytesPerSec);
        this.adminClient = new AdminClient(cluster,
                                           new AdminClientConfig().setMaxThreads(cluster.getNumberOfNodes())
                                                                  .setMaxConnectionsPerNode(1));
    }

    public void run() {
        logger.debug("Pushing slop...");

        SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> entries = null;
        try {
            StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
            entries = slopStore.entries();

            while(entries.hasNext()) {
                if(Thread.interrupted())
                    throw new InterruptedException("Slop pusher job cancelled!");

                try {
                    Pair<ByteArray, Versioned<Slop>> keyAndVal;
                    try {
                        keyAndVal = entries.next();
                        Versioned<Slop> versioned = keyAndVal.getSecond();
                        int nodeId = versioned.getValue().getNodeId();
                        SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> slopQueue = slopQueues.get(nodeId);
                        if(slopQueue == null) {
                            slopQueue = new SynchronousQueue<Pair<ByteArray, Versioned<Slop>>>();
                            slopQueues.put(nodeId, slopQueue);
                            consumerExecutor.submit(new SlopConsumer(nodeId, slopQueue));
                        }
                        slopQueue.put(keyAndVal);
                        readThrottler.maybeThrottle(nBytesRead(keyAndVal));
                    } catch(Exception e) {
                        logger.error("Exception in the entries, escaping the loop ", e);
                        break;
                    }
                } catch(Exception e) {
                    logger.error(e, e);
                }
            }
            for(SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> queue: slopQueues.values())
                queue.put(END);
        } catch(Exception e) {
            logger.error(e, e);
        } finally {
            try {
                if(entries != null)
                    entries.close();
            } catch(Exception e) {
                logger.error("Failed to close entries.", e);
            }
        }
    }

    private int nBytesRead(Pair<ByteArray, Versioned<Slop>> keyAndVal) {
        return keyAndVal.getFirst().length() + nBytesWritten(keyAndVal);
    }

    private int nBytesWritten(Pair<ByteArray, Versioned<Slop>> keyAndVal) {
        int nBytes = 0;
        Versioned<Slop> slopVersioned = keyAndVal.getSecond();
        Slop slop = slopVersioned.getValue();
        nBytes += slop.getKey().length();
        nBytes += ((VectorClock) slopVersioned.getVersion()).sizeInBytes();
        switch(slop.getOperation()) {
            case PUT: {
                nBytes += slop.getValue().length;
                break;
            }
            case DELETE: {
                break;
            }
            default:
                logger.error("Unknown slop operation: " + slop.getOperation());
        }
        return nBytes;
    }

    private class SlopIterator extends AbstractIterator<Pair<String, Pair<ByteArray, Versioned<byte[]>>>> {

        private final SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> slopQueue;
        private volatile int writtenLast;

        public SlopIterator(SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> slopQueue) {
            this.slopQueue = slopQueue;
            writtenLast = 0;
        }

        @Override
        protected Pair<String, Pair<ByteArray, Versioned<byte[]>>> computeNext() {
            try {
                Pair<ByteArray, Versioned<Slop>> head = null;
                boolean shutDown = false;
                while(!shutDown) {
                    head = slopQueue.poll();

                    if(head == null) {
                        continue;
                    }

                    if(head.equals(END)) {
                        shutDown = true;
                    } else {
                        writeThrottler.maybeThrottle(writtenLast);
                        writtenLast = nBytesWritten(head);
                        
                        Versioned<Slop> slopVersioned = head.getSecond();
                        Slop slop = slopVersioned.getValue();
                        return Pair.create(slop.getStoreName(),
                                           Pair.create(slop.getKey(),
                                                       new Versioned<byte[]>(slop.getValue(),
                                                                             slopVersioned.getVersion())));
                    }
                }
                return endOfData();
            } catch(Throwable t) {
                throw new RuntimeException("consumer failed inside iterator", t);
            }

        }
    }

    private class SlopConsumer implements Runnable {

        private final int nodeId;
        private final SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> slopQueue;

        public SlopConsumer(int nodeId,
                            SynchronousQueue<Pair<ByteArray, Versioned<Slop>>> slopQueue) {
            this.nodeId = nodeId;
            this.slopQueue = slopQueue;
        }
        public void run() {
            adminClient.updateStoreEntries(nodeId, new SlopIterator(slopQueue));
        }
    }
}
