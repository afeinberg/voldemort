package voldemort.store.routed.action;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

import java.util.Date;
import java.util.Map;
import java.util.Queue;


public class PerformHintedHandoff extends AbstractAction<ByteArray, Void, PutPipelineData> {
    private final StoreDefinition storeDefinition;
    private final Versioned<byte[]> versioned;
    private final ByteArray key;
    private final FailureDetector failureDetector;

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Event completeEvent,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                StoreDefinition storeDefinition,
                                FailureDetector failureDetector) {
        super(pipelineData, completeEvent);
        this.storeDefinition = storeDefinition;
        this.key = key;
        this.versioned = versioned;
        this.failureDetector = failureDetector;
    }

    public void execute(Pipeline pipeline) {
        Queue<Integer> slopQueue = pipelineData.getSlopQueue();
        Map<Node, Store<ByteArray, Slop>> slopStores = pipelineData.getSlopStores();

        while (!slopQueue.isEmpty()) {
            int nodeId = slopQueue.remove();
            
            if (logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + nodeId);

            Slop slop = new Slop(storeDefinition.getName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versioned.getValue(),
                                 nodeId,
                                 new Date());

            Node slopDestination = null;
            for (Map.Entry<Node, Store<ByteArray, Slop>> e: slopStores.entrySet()) {
                Node slopNode = e.getKey();
                Store<ByteArray,Slop> slopStore = e.getValue();
                long start = System.nanoTime();
                long requestTime;
                try {
                    slopStore.put(slop.makeKey(), new Versioned<Slop>(slop, versioned.getVersion()));
                    requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                    failureDetector.recordSuccess(slopNode, requestTime);
                    slopDestination = slopNode;

                    if (logger.isTraceEnabled())
                        logger.trace("Slop put on " + slopNode.getId() + " succeeded");

                    break;
                } catch (ObsoleteVersionException ove) {
                    logger.warn("ObsoleteVersionException when storing a slop ", ove);
                } catch (UnreachableStoreException use) {
                    requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                    handleResponseError(use, slopNode, requestTime, pipeline, failureDetector);
                }
            }

            if (slopDestination != null)
                slopStores.remove(slopDestination);
        }

        if (pipelineData.getFatalError() != null)
            pipeline.addEvent(Event.ERROR);
        else
            pipeline.addEvent(completeEvent);
    }
}
