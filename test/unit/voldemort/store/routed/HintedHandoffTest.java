package voldemort.store.routed;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import voldemort.MutableStoreVerifier;
import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.routing.RoutingStrategyType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.stats.StatTrackingStore;
import voldemort.utils.ByteArray;

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
            // TODO: extend ForceFailStore to fail on demand
            
            Store<ByteArray, byte[]> subStore = new InMemoryStorageEngine<ByteArray, byte[]>(STORE_NAME);
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
        // TODO: finish this as a failing test

        RoutedStore routedStore = routedStoreFactory.create(cluster,
                                                            storeDef,
                                                            subStores,
                                                            true,
                                                            failureDetector);
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
