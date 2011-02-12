package voldemort.store.routed.action;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.TimedPipelineData;
import voldemort.store.slop.HintedHandoff;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;

import java.util.List;
import java.util.Map;

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
    }


    public void execute(Pipeline pipeline) {
        List<Node> nodes = pipelineData.getNodes();
    }
}
