package voldemort.store.routed;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Before;
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
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.ForceFailStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static voldemort.VoldemortTestConstants.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HintedHandoffTest {

    private final static String STORE_NAME = "test";
    private final static int NUM_THREADS = 4;
    private final static int NUM_NODES = 9;
    private final static int KEY_LENGTH = 512;
    private final static int VALUE_LENGTH = 1024;


    private final Class<FailureDetector> failureDetectorClass;
    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private RoutedStoreFactory routedStoreFactory;
    private Map<Integer, Store<ByteArray, byte[]>> subStores;

    public HintedHandoffTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { BannagePeriodFailureDetector.class } });
    }

    @Before
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        storeDef = ServerTestUtils.getStoreDef(STORE_NAME,
                                               3,
                                               2,
                                               1,
                                               3,
                                               2,
                                               RoutingStrategyType.CONSISTENT_STRATEGY);

        subStores = Maps.newHashMap();

        for (Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> subStore = new ForceFailStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(STORE_NAME));
            subStores.put(node.getId(), subStore);
        }

        setFailureDetector(subStores);
        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(true,
                                                    routedStoreThreadPool,
                                                    1000L);
    }

    @After
    public void tearDown() throws Exception {
        if (failureDetector != null)
            failureDetector.destroy();

        if (routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Test
    public void testHintedHandOff() throws Exception {
        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);

        Multimap<Integer, ByteArray> keysToNodes = HashMultimap.create();
        Map<ByteArray, byte[]> keyValues = Maps.newHashMap();

        RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();
        RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                       cluster);

        while (keysToNodes.keySet().size() < NUM_NODES) {
            ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

            int nodeId = routingStrategy.routeRequest(randomKey.get()).get(0).getId();
            keysToNodes.put(nodeId, randomKey);
            keyValues.put(randomKey, randomValue);
        }

        for (ByteArray key: keysToNodes.values()) {
            Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key));
            routedStore.put(key, versioned);
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
