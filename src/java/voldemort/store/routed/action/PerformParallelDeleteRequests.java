package voldemort.store.routed.action;


import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Response;
import voldemort.store.routed.WritePipelineData;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

import java.util.Date;
import java.util.Map;

public class PerformParallelDeleteRequests extends PerformParallelWriteRequests<Boolean, WritePipelineData<Boolean>> {

    public PerformParallelDeleteRequests(WritePipelineData<Boolean> pipelineData,
                                         Pipeline.Event completeEvent,
                                         ByteArray key,
                                         FailureDetector failureDetector,
                                         int preferred,
                                         int required,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStores,
                                         HintedHandoff hintedHandoff,
                                         Pipeline.Event insufficientWritesEvent,
                                         Pipeline.Event insufficientZonesEvent) {
        super(pipelineData,
              completeEvent,
              key,
              failureDetector,
              preferred,
              required,
              timeoutMs,
              nonblockingStores,
              hintedHandoff,
              insufficientWritesEvent,
              insufficientZonesEvent);
    }

    @Override
    protected Slop makeSlop(Node node) {
        return new Slop(pipelineData.getStoreName(),
                        Slop.Operation.DELETE,
                        key,
                        null,
                        null,
                        node.getId(),
                        new Date());
    }

    @Override
    protected void performRequest(NonblockingStore store, NonblockingStoreCallback callback) {
        store.submitDeleteRequest(key, pipelineData.getVersion(), callback, getTimeoutMs());
    }

    @Override
    protected void handleResponseSuccess(int nodeId, Response<ByteArray, Object> response) {
        Response<ByteArray, Boolean> responseCast = Utils.uncheckedCast(response);
        pipelineData.getResponses().add(responseCast);
    }
}
