/*
 * Copyright 2008-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.rebalance;

import com.google.common.collect.Multimap;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class RebalanceController {

    private static final int MAX_TRIES = 2;
    private static Logger logger = Logger.getLogger(RebalanceController.class);

    private final AdminClient adminClient;
    private final RebalanceClientConfig rebalanceConfig;

    public RebalanceController(String bootstrapUrl, RebalanceClientConfig rebalanceConfig) {
        this.adminClient = new AdminClient(bootstrapUrl, rebalanceConfig);
        this.rebalanceConfig = rebalanceConfig;
    }

    public RebalanceController(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new AdminClient(cluster, config);
        this.rebalanceConfig = config;
    }

    private ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    /**
     * Grabs the latest cluster definition and calls
     * {@link #rebalance(voldemort.cluster.Cluster, voldemort.cluster.Cluster)}
     * 
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final Cluster targetCluster) {
        Versioned<Cluster> currentVersionedCluster = RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                     adminClient);
        rebalance(currentVersionedCluster.getValue(), targetCluster);
    }

    /**
     * Voldemort dynamic cluster membership (rebalancing) mechanism. <br>
     * Migrates partitions across nodes to manage changes in cluster
     * membership. <br>
     * Takes target cluster as parameter, fetches the current cluster
     * configuration from the cluster, compares and makes a list of partitions
     * that eed to be transferred.<br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param currentCluster: current cluster configuration
     * @param targetCluster: target cluster configuration
     */
    public void rebalance(Cluster currentCluster, final Cluster targetCluster) {
        logger.debug("Current Cluster configuration:" + currentCluster);
        logger.debug("Target Cluster configuration:" + targetCluster);

        adminClient.setAdminClientCluster(currentCluster);

        final RebalanceClusterPlan rebalanceClusterPlan = new RebalanceClusterPlan(currentCluster,
                                                                                   targetCluster,
                                                                                   RebalanceUtils.getStoreNameList(currentCluster,
                                                                                                                   adminClient),
                                                                                   rebalanceConfig.isDeleteAfterRebalancingEnabled());
        logger.info(rebalanceClusterPlan);

        // add all new nodes to currentCluster and propagate to all
        currentCluster = getClusterWithNewNodes(currentCluster, targetCluster);
        adminClient.setAdminClientCluster(currentCluster);
        Node firstNode = currentCluster.getNodes().iterator().next();
        VectorClock latestClock = (VectorClock) RebalanceUtils.getLatestCluster(new ArrayList<Integer>(),
                                                                                adminClient)
                                                              .getVersion();
        RebalanceUtils.propagateCluster(adminClient,
                                        currentCluster,
                                        latestClock.incremented(firstNode.getId(),
                                                                System.currentTimeMillis()),
                                        new ArrayList<Integer>());

        int maxParallelRebalancing = rebalanceConfig.getMaxParallelRebalancing();
        ExecutorService stealerExecutor = createExecutors(maxParallelRebalancing);
        final int maxParallelDonors = rebalanceConfig.getMaxParallelDonors();
        final ExecutorService donorExecutor = createExecutors(maxParallelDonors *
                                                              maxParallelRebalancing);

        // start all threads
        for(int i = 0; i < maxParallelRebalancing; i++) {
            stealerExecutor.execute(new Runnable() {

                public void run() {
                    // pick one node to rebalance from queue
                    Queue<RebalanceNodePlan> taskQueue = rebalanceClusterPlan.getRebalancingTaskQueue();
                    while(!taskQueue.isEmpty()) {
                        RebalanceNodePlan task = rebalanceClusterPlan.getRebalancingTaskQueue()
                                                                     .poll();
                        if(null != task) {
                            int stealerNodeId = task.getStealerNode();
                            Multimap<Integer, RebalancePartitionsInfo> subTasks = task.tasksByDonor();
                            Semaphore semaphore = new Semaphore(maxParallelDonors);
                            for (Collection<RebalancePartitionsInfo> tasksForDonor: subTasks.asMap().values())
                                donorExecutor.execute(new RebalanceClientOperation(tasksForDonor,
                                                                                   semaphore,
                                                                                   stealerNodeId));
                        }
                    }

                    logger.info("Thread run() finished:\n");
                }

            });
        }
        
        executorShutDown(stealerExecutor);
        executorShutDown(donorExecutor);

    }

    private int startNodeRebalancing(RebalancePartitionsInfo rebalanceSubTask) {
        int nTries = 0;
        AlreadyRebalancingException exception = null;

        while(nTries < MAX_TRIES) {
            nTries++;
            try {
                return adminClient.rebalanceNode(rebalanceSubTask);
            } catch(AlreadyRebalancingException e) {
                logger.info("Node " + rebalanceSubTask.getStealerId()
                            + " is currently rebalancing will wait till it finish.");
                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              MetadataStore.SERVER_STATE_KEY,
                                              VoldemortState.NORMAL_SERVER.toString(),
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                exception = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing at node "
                                     + rebalanceSubTask.getStealerId() + " with rebalanceInfo:"
                                     + rebalanceSubTask, exception);
    }

    private void executorShutDown(ExecutorService executorService) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                             TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }

    /* package level function to ease of unit testing */

    /**
     * Perform an atomic commit or revert for changing partition ownership:
     * <ol>
     * <li>Changes and modifies <code>adminClient</code> with the updated cluster</li>
     * <li>Creates new cluster metadata by moving partitions list passed in
     * as <code>rebalanceStealInfo</code> and propagates it to all the nodes</li>
     * <li>Reverts all changes if copying fails to required nodes (stealer and donor)</li>
     * <li>Holds a lock until the commit/revert finishes.</li>
     * </ol>
     *
     * @param stealerNode Node copy data from
     * @param rebalanceStealInfo Current rebalance sub task
     * @throws Exception If we are unable to propagate the cluster definition to donor and stealer
     */
    void commitClusterChanges(Node stealerNode, RebalancePartitionsInfo rebalanceStealInfo)
            throws Exception {
        synchronized(adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(Arrays.asList(donorNode.getId(),
                                                                                             rebalanceStealInfo.getStealerId()),
                                                                               adminClient);
            VectorClock latestClock = (VectorClock) latestCluster.getVersion();

            // apply changes and create new updated cluster.
            Cluster updatedCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                         stealerNode,
                                                                         donorNode,
                                                                         // use steal master partitions to update cluster
                                                                         rebalanceStealInfo.getStealMasterPartitions());
            // increment clock version on stealerNodeId
            latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
            try {
                // propagates changes to all nodes.
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                Arrays.asList(stealerNode.getId(),
                                                              rebalanceStealInfo.getDonorId()));

                // set new cluster in adminClient
                adminClient.setAdminClientCluster(updatedCluster);
            } catch(Exception e) {
                // revert cluster changes.
                updatedCluster = currentCluster;
                latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                new ArrayList<Integer>());

                throw e;
            }

            adminClient.setAdminClientCluster(updatedCluster);
        }
    }

    private Cluster getClusterWithNewNodes(Cluster currentCluster, Cluster targetCluster) {
        ArrayList<Node> newNodes = new ArrayList<Node>();
        for(Node node: targetCluster.getNodes()) {
            if(!RebalanceUtils.containsNode(currentCluster, node.getId())) {
                // add stealerNode with empty partitions list
                newNodes.add(RebalanceUtils.updateNode(node, new ArrayList<Integer>()));
            }
        }
        return RebalanceUtils.updateCluster(currentCluster, newNodes);
    }

    private final class RebalanceClientOperation implements Runnable {
        private final Collection<RebalancePartitionsInfo> tasks;
        private final Semaphore semaphore;
        private final int stealerNodeId;


        public RebalanceClientOperation(Collection<RebalancePartitionsInfo> tasks,
                                        Semaphore semaphore,
                                        int stealerNodeId) {
            this.tasks = tasks;
            this.semaphore = semaphore;
            this.stealerNodeId = stealerNodeId;
        }

        public void run() {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            try {
                for(RebalancePartitionsInfo task: tasks)
                    processTask(task);
            } finally {
                semaphore.release();
            }
        }

        /**
         * Execute an individual rebalance
         * @param task Current task
         */
        private void processTask(RebalancePartitionsInfo task) {
            logger.info("Starting rebalancing for stealerNode: " + stealerNodeId +
                        " with rebalanceInfo: " + task);

            try {
                int rebalanceAsyncId = startNodeRebalancing(task);

                try {
                    commitClusterChanges(adminClient.getAdminClientCluster()
                                                    .getNodeById(stealerNodeId),
                                         task);
                } catch(Exception e) {
                    if(-1 != rebalanceAsyncId)
                        adminClient.stopAsyncRequest(task.getStealerId(), rebalanceAsyncId);
                    throw e;
                }

                adminClient.waitForCompletion(task.getStealerId(),
                                              rebalanceAsyncId,
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                logger.info("Succesfully finished rebalance attempt: " + task);
            } catch(UnreachableStoreException e) {
                logger.error("StealerNode " + stealerNodeId
                             + " is unreachable, please make sure it is up and running.",
                             e);
            } catch(VoldemortRebalancingException e) {
                logger.error(e, e);
                for(Exception cause: e.getCauses())
                    logger.error(cause);
            } catch(Exception e) {
                logger.error("Rebalance attempt failed", e);
            }
        }
    }
}
