package voldemort.store.routed.action;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public class PerformHintedHandoff extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final FailureDetector failureDetector;

    private final SlopStoreFactory slopStoreFactory;

    private final Cluster cluster;

    private final Versioned<byte[]> versioned;

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Pipeline.Event completeEvent,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                FailureDetector failureDetector,
                                SlopStoreFactory slopStoreFactory,
                                Cluster cluster) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.slopStoreFactory = slopStoreFactory;
        this.cluster = cluster;
        this.versioned = versioned;
    }
    
    public void execute(Pipeline pipeline) {
        Set<Node> pipelineNodes = Sets.newHashSet(cluster.getNodes());
        Map<Integer, Store<ByteArray, Slop>> slopStores = Maps.newHashMapWithExpectedSize(pipelineNodes.size());

        for (Node node: pipelineNodes) {
            try {
                slopStores.put(node.getId(), slopStoreFactory.create(node.getId()));
            } catch (Exception e) {
                logger.warn("Unable to create a slop store for " + node, e);
            }
        }

        for (int nodeId: pipelineData.getFailedNodes()) {
            Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
            if (versionedCopy == null)
                versionedCopy = versioned;

            if (logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + nodeId
                             + ", store. " + pipelineData.getStoreName() + " key "
                             + key + ", value " + versionedCopy);

            boolean persisted = false;

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versionedCopy.getValue(),
                                 nodeId,
                                 new Date());

            Set<Node> used = Sets.newHashSetWithExpectedSize(pipelineNodes.size());
            for (Node slopNode: pipelineNodes) {
                int slopNodeId = slopNode.getId();
                Store<ByteArray,Slop> slopStore = slopStores.get(slopNodeId);

                if (slopNodeId != nodeId && failureDetector.isAvailable(slopNode)) {
                    long start = System.currentTimeMillis();
                    try {
                        if (logger.isTraceEnabled())
                            logger.trace("Writing slop " + slop);

                        slopStore.put(slop.makeKey(),
                                      new Versioned<Slop>(slop, versionedCopy.getVersion()));

                        persisted = true;
                        failureDetector.recordSuccess(slopNode, System.currentTimeMillis() - start);

                        used.add(slopNode);
                        if (logger.isTraceEnabled())
                            logger.trace("Finished hinted handoff for " + nodeId
                                         + " writing slop to " + slopNodeId);

                        break;
                    } catch (UnreachableStoreException e) {
                        failureDetector.recordException(slopNode, System.currentTimeMillis() - start, e);
                        logger.warn("Error during hinted handoff ", e);
                    } catch (VoldemortException e) {
                        logger.error("Unexpected exception during hinted handoff ", e);
                    }
                }
            }

            if (pipelineNodes.size() > used.size()) {
                for (Node usedNode: used)
                    pipelineNodes.remove(usedNode);
            }

            Exception e = pipelineData.getFatalError();
            if (e != null) {
                if (persisted)
                    pipelineData.setFatalError(new UnreachableStoreException("Put operation failed on node "
                                                                             + nodeId
                                                                             + ", but has been persisted to slop storage for eventual replication.",
                                                                             e));
                else
                    pipelineData.setFatalError(new InsufficientOperationalNodesException("All slop servers are unavailable from node " + nodeId + ".", e));
            }

        }
        
        pipeline.addEvent(completeEvent);
    }
}
