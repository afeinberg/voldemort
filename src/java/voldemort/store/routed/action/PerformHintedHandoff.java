package voldemort.store.routed.action;

import com.google.common.collect.Lists;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStoreFactory;
import voldemort.utils.ByteArray;

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
                logger.trace("Performing hinted handoff for node id " + nodeId
                             + " store " + pipelineData.getStoreName());

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 pipelineData.getVersionedCopy().getValue(),
                                 nodeId,
                                 new Date());
        }

    }
}
