package voldemort.store.routed;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import voldemort.MutableStoreVerifier;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.cluster.failuredetector.ThresholdFailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.SlopPusherJob;
import voldemort.store.ForceFailStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.DummySlopStoreFactory;
import voldemort.store.slop.MockSlopStoreFactory;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

import java.util.*;
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
    private final static int NUM_FAILED_NODES = 4;
    private final static int REPLICATION_FACTOR = 3;
    private final static int P_READS = 2;
    private final static int R_READS = 1;
    private final static int P_WRITES = 3;
    private final static int R_WRITES = 2;
    private final static int KEY_LENGTH = 512;
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
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class }, {ThresholdFailureDetector.class} });
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

        for (Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> subStore = new ForceFailStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(STORE_NAME));
            subStores.put(node.getId(), subStore);
        }

        setFailureDetector(subStores);

        for (Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = new StoreRepository();
            storeRepo.addLocalStore(subStores.get(nodeId));

            for (int i = 0; i < NUM_NODES; i++) {
                if (i != nodeId)
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

    /**
     * This is a known failing test. Will pass once hinted handoff is implemented.
     */
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

        Multimap<Integer, ByteArray> keysToNodes = HashMultimap.create();
        Map<ByteArray, byte[]> keyValues = Maps.newHashMap();

        RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();
        RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                       cluster);

        while (keysToNodes.keySet().size() < NUM_NODES) {
            ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

            for (Node node: routingStrategy.routeRequest(randomKey.get()))
                keysToNodes.put(node.getId(), randomKey);
            
            keyValues.put(randomKey, randomValue);
        }

        Set<Integer> failedNodes = Sets.newHashSet();
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
        for (Map.Entry<Integer, ByteArray> entry: keysToNodes.entries()) {
            int nodeId = entry.getKey();
            ByteArray key = entry.getValue();

            if (!failedKeys.contains(key)) {
                if (failedNodes.contains(nodeId))
                    failedKeys.add(key);

                try {
                    Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key));
                    routedStore.put(key, versioned);
                } catch (Exception e) {

                }
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

                System.out.println("got a value for " + slop);
            }
        }

        for (ByteArray failedKey: failedKeys) {
            System.out.println("verifying key " + failedKey);
            byte[] failedValue = keyValues.get(failedKey);
            byte[] actualValue = dataInSlops.get(failedKey);
            assertNotNull("data stored in slops", actualValue);
            assertEquals("correct data stored in slops", 0, ByteUtils.compare(actualValue, failedValue));
        }

        for (int nodeId: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(nodeId);
            forceFailStore.setFailAll(false);
            logger.info("Stopped failing requests to " + nodeId);
        }

        for (SlopPusherJob job: slopPusherJobs.values())
            pusherThreadPool.execute(job);

        pusherThreadPool.shutdown();
        pusherThreadPool.awaitTermination(5, TimeUnit.SECONDS);

        for (Map.Entry<ByteArray, byte[]> entry: keyValues.entrySet()) {
            List<Versioned<byte[]>> versionedValues = routedStore.get(entry.getKey());

            assertTrue(versionedValues.size() > 0);
            assertEquals("slop stores have correctly been pushed",
                         entry.getValue(),
                         versionedValues.get(0).getValue());
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
        failureDetectorConfig.setBannagePeriod(1000);
        failureDetectorConfig.setNodes(cluster.getNodes());
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }
}
