package voldemort.store.routed.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import voldemort.versioning.Versioned;

import java.util.Date;
import java.util.List;
import java.util.Map;


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
        List<Store<ByteArray, Slop>> slopStores = Lists.newArrayList();

        for (Node node: pipelineData.getNodes()) {
            try {
                slopStores.add(slopStoreFactory.create(node.getId()));
            } catch (Exception e) {
                logger.warn("Unable to create a slop store for " + node, e);
            }
        }

        for (int nodeId: pipelineData.getFailedNodes()) {
            if (logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + nodeId
                             + ", store " + pipelineData.getStoreName());

            boolean persisted = false;
            
            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 pipelineData.getVersionedCopy().getValue(),
                                 nodeId,
                                 new Date());

            for (Store<ByteArray, Slop> slopStore: slopStores) {
                try {
                    slopStore.put(slop.makeKey(),
                                  new Versioned<Slop>(slop, pipelineData.getVersionedCopy().getVersion()));
                    persisted = true;
                    break;
                } catch (UnreachableStoreException e) {
                    pipelineData.recordFailure(e);
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

    }
}
