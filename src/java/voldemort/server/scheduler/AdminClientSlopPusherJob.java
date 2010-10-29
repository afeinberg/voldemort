package voldemort.server.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

public class AdminClientSlopPusherJob {

    private final static Logger logger = Logger.getLogger(AdminClientSlopPusherJob.class.getName());

    private final Cluster cluster;
    private final FailureDetector failureDetector;
    private final StoreRepository storeRepo;
    private final long maxWriteBytesPerSec;

    private final Map<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> slops;
    private final ExecutorService consumerExecutor;
    private final Set<Integer> consumers;

    public AdminClientSlopPusherJob(Cluster cluster,
                                    FailureDetector failureDetector,
                                    StoreRepository storeRepo,
                                    long maxWriteBytesPerSec) {
        this.cluster = cluster;
        this.failureDetector = failureDetector;
        this.storeRepo = storeRepo;
        this.maxWriteBytesPerSec = maxWriteBytesPerSec;
        this.slops = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        this.consumerExecutor = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        this.consumers = Sets.newHashSetWithExpectedSize(cluster.getNumberOfNodes());
    }

    public void run() {
        logger.debug("Pushing slop...");

        SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;
        try {
            StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
            iterator = slopStore.entries();
            EventThrottler eventThrottler = new EventThrottler(maxWriteBytesPerSec);

            while(iterator.hasNext()) {
                if(Thread.interrupted())
                    throw new InterruptedException("Slop pusher job cancelled!");

                try {
                    Pair<ByteArray, Versioned<Slop>> keyAndVal;
                    try {
                        keyAndVal = iterator.next();
                        Versioned<Slop> versioned = keyAndVal.getSecond();
                        Slop slop = versioned.getValue();
                        int nodeId = slop.getNodeId();
                        Node node = cluster.getNodeById(nodeId);

                        
                    } catch(Exception e) {
                        logger.error("Exception in the iterator, escaping the loop ", e);
                        break;
                    }
                } catch(Exception e) {
                    logger.error(e, e);
                }
            }
        } catch(Exception e) {
            logger.error(e, e);
        } finally {
            try {
                if(iterator != null)
                    iterator.close();
            } catch(Exception e) {
                logger.error("Failed to close iterator.", e);
            }
        }
    }

}
