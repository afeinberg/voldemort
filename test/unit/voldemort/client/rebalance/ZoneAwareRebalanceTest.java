package voldemort.client.rebalance;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Verify that rebalancing works when zone routing is enabled
 *
 */
public class ZoneAwareRebalanceTest {

    private final static Logger logger = Logger.getLogger(ZoneAwareRebalanceTest.class);
    private final static String storeName = "test";
    private final static String storeDefFile = "test/common/voldemort/config/two-stores.xml";

    private List<StoreDefinition> storeDefList;

    @Before
    public void setUp() throws IOException {
        storeDefList = new StoreDefinitionsMapper().readStoreList(new File(storeDefFile));
    }

    @Test
    public void testAddNodeInOneZone() {
        Cluster originalCluster = ServerTestUtils.getLocalClusterWithZones(4,
                                                                           new int[][] { { 0, 2, 9 },
                                                                                         { 1, 3, 8 },
                                                                                         { 4, 6, 10 },
                                                                                         { 5, 7, 11 } },
                                                                           new int[][] { { 0, 1 },
                                                                                         { 2, 3 } });
        Cluster targetCluster = ServerTestUtils.getLocalClusterWithZones(5,
                                                                         new int[][] { { 0, 2, 9 },
                                                                                       { 1, 3, 8 },
                                                                                       { 4, 6 },
                                                                                       { 5, 11 },
                                                                                       { 10, 7 } },
                                                                         new int[][] { { 0, 1 },
                                                                                       { 2, 3, 4 } });
        RebalanceClusterPlan clusterPlan = new RebalanceClusterPlan(originalCluster,
                                                                    targetCluster,
                                                                    storeDefList,
                                                                    true,
                                                                    null);
        logger.info("RebalanceClusterPlan: " + clusterPlan);
    }

    @Test
    public void testProxyGetsWithZones() {
        Cluster originalCluster = ServerTestUtils.getLocalClusterWithZones(4,
                                                                           new int[][] { { 0, 2, 9 },
                                                                                         { 1, 3, 8 },
                                                                                         { 4, 6, 10 },
                                                                                         { 5, 7, 11 } },
                                                                           new int[][] { { 0, 1 },
                                                                                         { 2, 3 } });
        Cluster targetCluster = ServerTestUtils.getLocalClusterWithZones(5,
                                                                         new int[][] { { 0, 2, 9 },
                                                                                       { 1, 3, 8 },
                                                                                       { 4, 6 },
                                                                                       { 5, 11 },
                                                                                       { 10, 7 } },
                                                                         new int[][] { { 0, 1 },
                                                                                       { 2, 3, 4 } });
    }

    @Test
    public void testAddNodesInBothZones() {
        Cluster originalCluster = ServerTestUtils.getLocalClusterWithZones(4,
                                                                           new int[][] { { 0, 2, 9 },
                                                                                         { 1, 3, 8 },
                                                                                         { 4, 6, 10 },
                                                                                         { 5, 7, 11 } },
                                                                           new int[][] { { 0, 1 },
                                                                                         { 2, 3 } });
         Cluster targetCluster = ServerTestUtils.getLocalClusterWithZones(6,
                                                                          new int[][] { { 0, 9 },
                                                                                        { 1, 3 },
                                                                                        { 4, 6 },
                                                                                        { 5, 11 },
                                                                                        { 10, 7 },
                                                                                        { 2, 8 } },
                                                                          new int[][] { { 0, 1, 5 },
                                                                                        { 2, 3, 4 } });

    }
}
