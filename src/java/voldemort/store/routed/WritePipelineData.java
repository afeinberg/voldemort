package voldemort.store.routed;

import voldemort.cluster.Node;
import voldemort.versioning.Version;

public class WritePipelineData<T> extends BasicPipelineData<T> {

    private Node master;

    private Version version;

    private long startTimeNs;

    public Node getMaster() {
        return master;
    }

    public void setMaster(Node master) {
        this.master = master;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public long getStartTimeNs() {
        return startTimeNs;
    }

    public void setStartTimeNs(long startTimeNs) {
        this.startTimeNs = startTimeNs;
    }
}
