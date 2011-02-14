package voldemort.store.routed.action;

import org.apache.log4j.Level;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.TimedPipelineData;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PerformParallelDeleteRequests
        extends AbstractKeyBasedAction<ByteArray, Boolean, TimedPipelineData<Boolean>> {

    private final Version version;
    private final FailureDetector failureDetector;
    private final int preferred;
    private final int required;
    private final long timeoutMs;
    private final Map<Integer, NonblockingStore> nonblockingStoreMap;
    private final HintedHandoff hintedHandoff;
    private final Pipeline.Event insufficientRequestsEvent;
    private final Pipeline.Event insufficientZonesEvent;
    private final boolean enableHintedHandoff;

    public PerformParallelDeleteRequests(TimedPipelineData<Boolean> pipelineData,
                                         Pipeline.Event completeEvent,
                                         ByteArray key,
                                         Version version,
                                         FailureDetector failureDetector,
                                         int preferred,
                                         int required,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStoreMap,
                                         HintedHandoff hintedHandoff,
                                         Pipeline.Event insufficientRequestsEvent,
                                         Pipeline.Event insufficientZonesEvent) {
        super(pipelineData, completeEvent, key);
        this.version = version;
        this.failureDetector = failureDetector;
        this.preferred = preferred;
        this.required = required;
        this.timeoutMs = timeoutMs;
        this.nonblockingStoreMap = nonblockingStoreMap;
        this.hintedHandoff = hintedHandoff;
        this.insufficientRequestsEvent = insufficientRequestsEvent;
        this.insufficientZonesEvent = insufficientZonesEvent;
        this.enableHintedHandoff = hintedHandoff != null;
    }

    public void execute(final Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();

        int attempts = nodes.size();
        int blocks = preferred;

        final Map<Integer, Response<ByteArray, Object>> responseMap = new ConcurrentHashMap<Integer, Response<ByteArray,Object>>();
        final CountDownLatch attemptsLatch = new CountDownLatch(attempts);
        final CountDownLatch blocksLatch = new CountDownLatch(blocks);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                        + " operations in parallel");

        for(int i = 0; i < attempts; i++) {
            final Node node = nodes.get(i);
            pipelineData.incrementNodeIndex();

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {
                public void requestComplete(Object result, long requestTime) {
                    if(logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                    + " response received (" + requestTime + " ms.) from node "
                                    + node.getId());

                    Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                                                                           key,
                                                                                           result,
                                                                                           requestTime);
                    responseMap.put(node.getId(), response);

                    if(enableHintedHandoff && pipeline.isFinished()) {
                        if(response.getValue() instanceof UnreachableStoreException) {
                            pipelineData.addFailedNode(node);
                            Slop slop = new Slop(pipelineData.getStoreName(),
                                                 Slop.Operation.DELETE,
                                                 key,
                                                 null,
                                                 null,
                                                 node.getId(),
                                                 new Date());
                            hintedHandoff.sendHintSerial(node, version, slop);
                        }
                    }

                    attemptsLatch.countDown();
                    blocksLatch.countDown();

                    if(logger.isTraceEnabled())
                        logger.trace(attemptsLatch.getCount() + " attempts remaining. Will block "
                                     + " for " + blocksLatch.getCount() + " more");

                    if(pipeline.isFinished() && response.getValue() instanceof Exception
                       && !(response.getValue() instanceof ObsoleteVersionException)) {
                        handleResponseError(response, pipeline, failureDetector);
                    }
                }
            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                            + " request on node " + node.getId());

            NonblockingStore store = nonblockingStoreMap.get(node.getId());
            store.submitDeleteRequest(key, version, callback, timeoutMs);
        }


        if (collectResponses(pipeline, responseMap, blocksLatch))
            return;

        boolean quorumSatisfied = true;
        if(pipelineData.getSuccesses() < required) {
            if(collectResponses(pipeline, responseMap, attemptsLatch))
                return;
        }

        if(pipelineData.getSuccesses() < required) {
            if(insufficientRequestsEvent != null)
                pipeline.addEvent(insufficientRequestsEvent);
            else {
                pipelineData.setFatalError(new InsufficientOperationalNodesException(required
                                                                                     + " "
                                                                                     + pipeline.getOperation()
                                                                                               .getSimpleName()
                                                                                     + " s required, but only "
                                                                                     + pipelineData.getSuccesses()
                                                                                     + " succeeded",
                                                                                     pipelineData.getFailures()));
                pipeline.abort();
            }
            quorumSatisfied = false;
        }

        if(quorumSatisfied) {
            if(pipelineData.getZonesRequired() != null) {
                int zonesSatisfied = pipelineData.getZoneResponses().size();
                if(zonesSatisfied >= (pipelineData.getZonesRequired() + 1))
                    pipeline.addEvent(completeEvent);
                else {
                    if(collectResponses(pipeline, responseMap, attemptsLatch))
                        return;

                    if(pipelineData.getZoneResponses().size() >= (pipelineData.getZonesRequired() + 1))
                        pipeline.addEvent(completeEvent);
                    else {
                        if(insufficientZonesEvent != null)
                            pipeline.addEvent(insufficientZonesEvent);
                        else {
                            pipelineData.setFatalError(new InsufficientZoneResponsesException(pipelineData.getZonesRequired()
                                                                                              + " "
                                                                                              + pipeline.getOperation()
                                                                                                        .getSimpleName()
                                                                                              + "s required from different zones, but only "
                                                                                              + zonesSatisfied
                                                                                              + " zones succeeded",
                                                                                              pipelineData.getFailures()));
                            pipeline.abort();
                        }
                    }
                }
            } else {
                pipeline.addEvent(completeEvent);
            }
        }
    }

    private boolean collectResponses(Pipeline pipeline,
                                     Map<Integer, Response<ByteArray, Object>> responses,
                                     CountDownLatch blocksLatch) {
        long elapsedNs = System.nanoTime() - pipelineData.getStartTimeNs();
        long remainingNs = (timeoutMs * Time.NS_PER_MS) - elapsedNs;
        if(remainingNs > 0) {
            try {
                blocksLatch.await(remainingNs, TimeUnit.NANOSECONDS);
            } catch(InterruptedException e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }

        for(Map.Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
            Response<ByteArray, Object> response = responseEntry.getValue();
            if(response.getValue() instanceof Exception) {
                if(handleResponseError(response, pipeline, failureDetector))
                    return true;
            } else {
                pipelineData.incrementSuccesses();
                Response<ByteArray, Boolean> responseCast = Utils.uncheckedCast(response);
                pipelineData.getResponses().add(responseCast);
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                pipelineData.getZoneResponses().add(response.getNode().getZoneId());
                responses.remove(responseEntry.getKey());
            }
        }
        return false;
    }


}
