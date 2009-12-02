package voldemort.store.slop;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;

import java.util.Set;

/**
 * @author afeinberg
 */
public class HintedHandoff {
    private final Cluster cluster;
    private final RoutingStrategy routingStrategy;
    private int lastChosen = -1;

    public HintedHandoff(Cluster cluster, RoutingStrategy routingStrategy) {
        this.cluster = cluster;
        this.routingStrategy = routingStrategy;
    }

    public synchronized Node getSlopNode(byte[] key) {
        Set<Integer> prefListIds = ImmutableSet.copyOf(Lists.transform(routingStrategy.routeRequest(key),
                new Function<Node, Integer>() {
                    public Integer apply(Node input) {
                        return input.getId();
                    }
                }));

        if (lastChosen == cluster.getNumberOfNodes()-1)
            lastChosen = -1;

        for (int i=lastChosen+1; i < cluster.getNumberOfNodes(); i++) {
            if (!prefListIds.contains(i)) {
                lastChosen = i;
                return cluster.getNodeById(lastChosen);
            }
        }

        return null;
    }
    
}
