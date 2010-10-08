package voldemort.client.rebalance;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

/**
 * Tools to manipulate cluster geometries and verify them for correctness,
 * reliability and efficiency.
 * 
 */
public class RebalanceClusterTool {

    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final ListMultimap<Integer, Integer> masterToReplicas;

    /**
     * Constructs a <tt>RebalanceClusterTool</tt> for a given cluster and store
     * definition
     *
     * @param cluster Original cluster
     * @param storeDefinition Store definition to extract information such as
     *        replication-factor from. Typically this should be the store with
     *        the highest replication count.
     */
    public RebalanceClusterTool(Cluster cluster, StoreDefinition storeDefinition) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                             cluster);
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.masterToReplicas = createMasterToReplicas(cluster, routingStrategy);
    }

    public Multimap<Integer, Integer> getMasterToReplicas() {
        return masterToReplicas;
    }

    private ListMultimap<Integer, Integer> createMasterToReplicas(Cluster cluster,
                                                                  RoutingStrategy routingStrategy) {
        ListMultimap<Integer, Integer> lmm = ArrayListMultimap.create();
        
        for(int i = 0; i < cluster.getNumberOfPartitions(); i++) {
            for(int replica: routingStrategy.getReplicatingPartitionList(i)) {
                if(replica != i)
                    lmm.put(i, replica);
            }
        }

        return lmm;
    }

    public Multimap<Node, Integer> multipleCopies(Cluster newCluster) {
        Multimap<Node, Integer> copies = LinkedHashMultimap.create();
        for(Node n: newCluster.getNodes()) {
            List<Integer> partitions = n.getPartitionIds();
            for(int partition: partitions) {
                for(int replica: masterToReplicas.get(partition)) {
                    if(partitions.contains(replica)) {
                        if(!copies.get(n).contains(partition))
                            copies.put(n, partition);
                        copies.put(n, replica);
                    }
                }
            }
        }

        return copies;
    }

    public Multimap<Integer, Pair<Integer, Integer>> remappedReplicas(Cluster targetCluster) {
        ListMultimap<Integer, Pair<Integer, Integer>> remappedReplicas = ArrayListMultimap.create();
        ListMultimap<Integer, Pair<Integer, Integer>> oldClusterMap = createClusterMap(cluster);
        ListMultimap<Integer, Pair<Integer, Integer>> newClusterMap = createClusterMap(targetCluster);

        for(int partition: masterToReplicas.keySet()) {
            List<Pair<Integer, Integer>> oldReplicas = oldClusterMap.get(partition);
            List<Pair<Integer, Integer>> newReplicas = newClusterMap.get(partition);

            if(oldReplicas.size() != newReplicas.size())
                throw new IllegalStateException("replica count differs for partition " + partition);

            for(int i = 0; i < oldReplicas.size(); i++) {
                Pair<Integer, Integer> oldReplica = oldReplicas.get(i);
                if(!newReplicas.contains(oldReplica)) {
                    Pair<Integer, Integer> pair = new Pair<Integer, Integer>(oldReplica.getFirst(),
                                                                             newReplicas.get(i).getFirst());
                    remappedReplicas.put(partition, pair);
                }
            }
        }

        return remappedReplicas;
    }

    private ListMultimap<Integer, Pair<Integer, Integer>> createClusterMap(Cluster cluster) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                             cluster);
        Map<Integer, Integer> partitionToNode = createPartitionToNode(cluster);
        ListMultimap<Integer, Pair<Integer, Integer>> clusterMap = ArrayListMultimap.create();
        for(int partition = 0; partition < cluster.getNumberOfPartitions(); partition++) {
            for(int replica: routingStrategy.getReplicatingPartitionList(partition)) {
                if(replica != partition)
                    clusterMap.put(partition, new Pair<Integer, Integer>(replica, partitionToNode.get(replica)));
            }
        }
        return clusterMap;
    }

    private Map<Integer, Integer> createPartitionToNode(Cluster cluster) {
        Map<Integer, Integer> map = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            for(Integer partition: node.getPartitionIds())
                map.put(partition, node.getId());
        }
        return map;
    }
}
