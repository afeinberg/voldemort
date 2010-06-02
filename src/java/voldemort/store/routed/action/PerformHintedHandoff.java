package voldemort.store.routed.action;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.utils.ByteArray;

import java.util.Map;


public class PerformHintedHandoff extends
        AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final FailureDetector failureDetector;

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Pipeline.Event completeEvent,
                                ByteArray key,
                                FailureDetector failureDetector,
                                long timeoutMs,
                                Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
    }
    
    public void execute(Pipeline pipeline) {

    }
}
