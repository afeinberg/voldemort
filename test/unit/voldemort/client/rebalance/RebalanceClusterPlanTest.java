package voldemort.client.rebalance;

import com.google.common.collect.Sets;
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
import java.util.Queue;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class RebalanceClusterPlanTest {

    private final Logger logger = Logger.getLogger(RebalanceClusterPlanTest.class);
    private final static String storeDefFile = "test/common/voldemort/config/stores.xml";
    private List<StoreDefinition> storeDefs;

    @Before
    public void setUp() throws IOException {
        storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storeDefFile));
    }

    @Test
    public void testGetReplicationChanges() {
        Cluster originalCluster = ServerTestUtils.getLocalCluster(3,
                                                                  new int[][] {{0, 7, 8},
                                                                               {2, 3, 4},
                                                                               {1, 5, 6}});
        Cluster targetCluster = ServerTestUtils.getLocalCluster(4,
                                                                new int[][] {{7, 8},
                                                                             {3, 4},
                                                                             {1, 5, 6},
                                                                             {0, 2}});

        RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(originalCluster,
                                                                             targetCluster,
                                                                             storeDefs,
                                                                             false,
                                                                             null);
        if(logger.isDebugEnabled())
            logger.debug(rebalanceClusterPlan);

        Queue<RebalanceNodePlan> queue = rebalanceClusterPlan.getRebalancingTaskQueue();
        assertEquals("two steps in rebalance plan", queue.size(), 2);

        RebalanceNodePlan stepOne = queue.remove();
        RebalanceNodePlan stepTwo = queue.remove();
        assertEquals("stealer node at step 1 correct", stepOne.getStealerNode(), 1);
        assertEquals("stealer node at step 2 correct", stepTwo.getStealerNode(), 3);

        Set<Integer> donors = Sets.newHashSet();
        for(RebalancePartitionsInfo rebalancePartitionsInfo: stepOne.getRebalanceTaskList())
            donors.add(rebalancePartitionsInfo.getDonorId());
        assertTrue("copying partitions from node 2", donors.contains(2));

        donors.clear();
        for(RebalancePartitionsInfo rebalancePartitionsInfo: stepTwo.getRebalanceTaskList())
            donors.add(rebalancePartitionsInfo.getDonorId());
        assertTrue("copying partitions from node 0", donors.contains(0));
        assertTrue("copying partitions from node 1", donors.contains(1));
        assertTrue("copying partitions from node 2", donors.contains(2));
    }
}
