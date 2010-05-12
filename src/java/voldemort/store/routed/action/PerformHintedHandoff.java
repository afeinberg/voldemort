package voldemort.store.routed.action;

import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;


public class PerformHintedHandoff extends AbstractAction<ByteArray, Void, PutPipelineData> {

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Event completeEvent,
                                Versioned<byte[]> versioned) {
        super(pipelineData, completeEvent);
    }

    public void execute(Pipeline pipeline) {
        if (pipelineData.getFatalError() != null)
            pipeline.addEvent(Event.ERROR);
        else
            pipeline.addEvent(completeEvent);
    }
}
