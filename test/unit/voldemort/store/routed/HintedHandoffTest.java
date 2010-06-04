package voldemort.store.routed;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import voldemort.MutableStoreVerifier;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.*;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.SlopPusherJob;
import voldemort.store.*;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.slop.MockSlopStoreFactory;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static voldemort.VoldemortTestConstants.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HintedHandoffTest {

    private final static Logger logger = Logger.getLogger(HintedHandoffTest.class);

    private final static String STORE_NAME = "test";
    private final static String SLOP_STORE_NAME = "slop";
    private final static int NUM_THREADS = 4;
    private final static int NUM_NODES = 9;
    private final static int NUM_FAILED_NODES = 3;
    private final static int REPLICATION_FACTOR = 3;
    private final static int P_READS = 2;
    private final static int R_READS = 1;
    private final static int P_WRITES = 3;
    private final static int R_WRITES = 2;
    private final static int KEY_LENGTH = 64;
    private final static int VALUE_LENGTH = 1024;

    private final Class<FailureDetector> failureDetectorClass;

    private final Map<Integer, Store<ByteArray, byte[]>> subStores = Maps.newHashMap();
    private final Map<Integer, Store<ByteArray, Slop>> slopStores = Maps.newHashMap();
    private final Map<Integer, StoreRepository> storeRepos = Maps.newHashMap();
    private final Map<Integer, SlopPusherJob> slopPusherJobs = Maps.newHashMap();

    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private ExecutorService pusherThreadPool;
    private RoutedStoreFactory routedStoreFactory;

    public HintedHandoffTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class } ,
                                              { ThresholdFailureDetector.class } });
    }

    @Before
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        storeDef = ServerTestUtils.getStoreDef(STORE_NAME,
                                               REPLICATION_FACTOR,
                                               P_READS,
                                               R_READS,
                                               P_WRITES,
                                               R_WRITES,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);
        Random rand = new Random();
        for (Node node: cluster.getNodes()) {
            VoldemortException e;
            if (rand.nextInt() % 2 == 0)
                e = new VoldemortException("Operation failed");
            else
                e = new UnreachableStoreException("Node down");

            Store<ByteArray, byte[]> subStore = new ForceFailStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(STORE_NAME), e);
            subStores.put(node.getId(), subStore);
        }

        setFailureDetector(subStores);

        for (Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = new StoreRepository();
            storeRepo.addLocalStore(subStores.get(nodeId));

            for (int i = 0; i < NUM_NODES; i++) {
                storeRepo.addNodeStore(i, subStores.get(i));
            }

            StorageEngine<ByteArray, Slop> slopStorageEngine = new InMemoryStorageEngine<ByteArray, Slop>(SLOP_STORE_NAME);
            storeRepo.setSlopStore(slopStorageEngine);

            slopStores.put(nodeId, slopStorageEngine);
            storeRepos.put(nodeId, storeRepo);

            SlopPusherJob pusher = new SlopPusherJob(storeRepo);
            slopPusherJobs.put(nodeId, pusher);
        }

        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(true,
                                                    routedStoreThreadPool,
                                                    1000L);

        pusherThreadPool = Executors.newFixedThreadPool(NUM_NODES);
    }

    @After
    public void tearDown() throws Exception {
        if (failureDetector != null)
            failureDetector.destroy();

        if (routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();

        if (pusherThreadPool != null && !pusherThreadPool.isTerminated())
            pusherThreadPool.shutdown();
    }

    @Test
    public void testHintedHandOff() throws Exception {
        MockSlopStoreFactory slopStoreFactory = new MockSlopStoreFactory(slopStores);

        // Enable hinted handoff, but do not repair reads
        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            false,
                                                            true,
                                                            failureDetector,
                                                            slopStoreFactory);

        Multimap<ByteArray,Integer> keysToNodes = HashMultimap.create();
        Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();

        RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();
        RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                       cluster);

        while (keysToNodes.keySet().size() < NUM_NODES) {
            ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

            for (Node node: routingStrategy.routeRequest(randomKey.get()))
                keysToNodes.put(randomKey, node.getId());
            
            keyValues.put(randomKey, new ByteArray(randomValue));
        }

        Set<Integer> failedNodes = new CopyOnWriteArraySet<Integer>();
        Random rand = new Random();
        int i = rand.nextInt(NUM_NODES);

        for (int j=0; j < NUM_FAILED_NODES; j++)
            failedNodes.add((i + j) % NUM_NODES);

        for (int nodeId: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(nodeId);
            forceFailStore.setFailAll(true);
            logger.info("Started failing requests to " + nodeId);
        }

        Set<ByteArray> failedKeys = Sets.newHashSet();
        for (ByteArray key: keysToNodes.keySet()) {
            Iterable<Integer> nodeIds = keysToNodes.get(key);

            for (int n = 0; n < R_WRITES; n++) {
                int nodeId = Iterables.get(nodeIds, n);
                if (failedNodes.contains(nodeId)) {
                    failedKeys.add(key);
                    break;
                }
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                routedStore.put(key, versioned);
            } catch (Exception e) {
                logger.trace(e, e);
            }
            
        }

        Map<ByteArray, byte[]> dataInSlops = Maps.newHashMap();
        Set<ByteArray> slopKeys = Sets.newHashSet();

        byte[] opCode = new byte[] { Slop.Operation.PUT.getOpCode() };
        byte[] spacer = new byte[] { (byte) 0 };
        byte[] storeName = ByteUtils.getBytes(STORE_NAME, "UTF-8");

        for (ByteArray key: failedKeys)
            slopKeys.add(new ByteArray(ByteUtils.cat(opCode, spacer, storeName, spacer, key.get())));

        for (Store<ByteArray, Slop> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys);
            for (Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                dataInSlops.put(slop.getKey(), slop.getValue());
                logger.trace(slop);
            }
        }

        for (ByteArray failedKey: failedKeys) {
            logger.trace("verifying key " + failedKey);
            byte[] failedValue = keyValues.get(failedKey).get();
            byte[] actualValue = dataInSlops.get(failedKey);
            assertNotNull("data stored in slops", actualValue);
            assertEquals("correct data stored in slops", 0, ByteUtils.compare(actualValue, failedValue));
        }
    }

    public ForceFailStore<ByteArray, byte[]> getForceFailStore(int nodeId) {
        return (ForceFailStore <ByteArray, byte[]>) subStores.get(nodeId);
    }

    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[]>> subStores)
            throws Exception {
        if (failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(failureDetectorClass.getName());
        failureDetectorConfig.setBannagePeriod(100);
        failureDetectorConfig.setRequestLengthThreshold(10);
        failureDetectorConfig.setAsyncRecoveryInterval(5);
        failureDetectorConfig.setNodes(cluster.getNodes());
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    @Test
    public void testHintedHandoffSlopPusher() throws Exception {
        MockSlopStoreFactory slopStoreFactory = new MockSlopStoreFactory(slopStores);

        // Enable hinted handoff, but do not repair reads
        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            false,
                                                            true,
                                                            failureDetector,
                                                            slopStoreFactory);

        Multimap<ByteArray,Integer> keysToNodes = HashMultimap.create();
        Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();

        RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();
        RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                       cluster);

        while (keysToNodes.keySet().size() < NUM_NODES) {
            ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

            for (Node node: routingStrategy.routeRequest(randomKey.get()))
                keysToNodes.put(randomKey, node.getId());

            keyValues.put(randomKey, new ByteArray(randomValue));
        }

        Set<Integer> failedNodes = new CopyOnWriteArraySet<Integer>();
        Random rand = new Random();
        int i = rand.nextInt(NUM_NODES);

        for (int j=0; j < NUM_FAILED_NODES; j++)
            failedNodes.add((i + j) % NUM_NODES);

        for (int nodeId: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(nodeId);
            forceFailStore.setFailAll(true);
            logger.info("Started failing requests to " + nodeId);
        }

        Set<ByteArray> failedKeys = Sets.newHashSet();
        for (ByteArray key: keysToNodes.keySet()) {
            Iterable<Integer> nodeIds = keysToNodes.get(key);

            for (int n = 0; n < R_WRITES; n++) {
                int nodeId = Iterables.get(nodeIds, n);
                if (failedNodes.contains(nodeId)) {
                    failedKeys.add(key);
                    break;
                }
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                routedStore.put(key, versioned);
            } catch (Exception e) {
                logger.trace(e, e);
            }

        }

        Map<ByteArray, byte[]> dataInSlops = Maps.newHashMap();
        Set<ByteArray> slopKeys = Sets.newHashSet();

        byte[] opCode = new byte[] { Slop.Operation.PUT.getOpCode() };
        byte[] spacer = new byte[] { (byte) 0 };
        byte[] storeName = ByteUtils.getBytes(STORE_NAME, "UTF-8");

        for (ByteArray key: failedKeys)
            slopKeys.add(new ByteArray(ByteUtils.cat(opCode, spacer, storeName, spacer, key.get())));

        for (Store<ByteArray, Slop> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys);
            for (Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                dataInSlops.put(slop.getKey(), slop.getValue());
                logger.trace(slop);
            }
        }

        for (int nodeId: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(nodeId);
            forceFailStore.setFailAll(false);
            logger.info("Stopped failing requests to " + nodeId);
        }

        while (!failedNodes.isEmpty()) {
            for (int nodeId: failedNodes) {
                if (failureDetector.isAvailable(cluster.getNodeById(nodeId)))
                    failedNodes.remove(nodeId);
            }
        }

        Thread.sleep(100);

        for (SlopPusherJob job: slopPusherJobs.values())
            pusherThreadPool.submit(job);

        pusherThreadPool.shutdown();
        pusherThreadPool.awaitTermination(5, TimeUnit.SECONDS);

        for (Map.Entry<ByteArray, ByteArray> entry: keyValues.entrySet()) {
            List<Versioned<byte[]>> versionedValues = routedStore.get(entry.getKey());

            assertTrue(versionedValues.size() > 0);
            assertEquals("slop stores have correctly been pushed",
                         entry.getValue(),
                         new ByteArray(versionedValues.get(0).getValue()));
        }
    }
}
