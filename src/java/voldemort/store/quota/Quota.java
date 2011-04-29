package voldemort.store.quota;

import voldemort.annotations.concurrency.Immutable;

@Immutable
public class Quota {

    private final long softLimit;
    private final long hardLimit;

    public Quota(long softLimit, long hardLimit) {
        this.softLimit = softLimit;
        this.hardLimit = hardLimit;
    }

    public long getSoftLimit() {
        return softLimit;
    }

    public long hardLimit() {
        return hardLimit;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;

        Quota quota = (Quota) o;

        if(hardLimit != quota.hardLimit) return false;
        if(softLimit != quota.softLimit) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (softLimit ^ (softLimit >>> 32));
        result = 31 * result + (int) (hardLimit ^ (hardLimit >>> 32));
        return result;
    }
}
