package voldemort.store.routed.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class PerformHintedHandoff extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final FailureDetector failureDetector;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final SlopStoreFactory slopStoreFactory;

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Pipeline.Event completeEvent,
                                ByteArray key,
                                FailureDetector failureDetector,
                                long timeoutMs,
                                Map<Integer, NonblockingStore> nonblockingStores,
                                SlopStoreFactory slopStoreFactory) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.slopStoreFactory = slopStoreFactory;
    }
    
    public void execute(Pipeline pipeline) {
        Map<Integer, Store<ByteArray, Slop>> slopStores = Maps.newHashMap();

        for (Node node: pipelineData.getNodes()) {
            try {
                slopStores.put(node.getId(), slopStoreFactory.create(node.getId()));
            } catch (Exception e) {
                logger.warn("Unable to create a slop store for " + node, e);
            }
        }

        for (int nodeId: pipelineData.getFailedNodes()) {
            if (logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + nodeId
                             + ", store " + pipelineData.getStoreName());

            boolean persisted = false;
            Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versionedCopy.getValue(),
                                 nodeId,
                                 new Date());

            for (Map.Entry<Integer, Store<ByteArray, Slop>> entry: slopStores.entrySet()) {
                Store<ByteArray,Slop> slopStore = entry.getValue();
                int slopNodeId = entry.getKey();

                if (slopNodeId != nodeId) {
                    try {
                        slopStore.put(slop.makeKey(),
                                      new Versioned<Slop>(slop, versionedCopy.getVersion()));
                        persisted = true;

                        if (logger.isTraceEnabled())
                            logger.trace("Finished hinted handoff for " + nodeId
                                         + " writing slop to " + slopNodeId);

                        break;
                    } catch (UnreachableStoreException e) {
                        logger.warn("Error during hinted handoff ", e);
                        pipelineData.recordFailure(e);
                    }
                }
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
