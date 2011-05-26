package voldemort.store.quota;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.stats.RequestCounter;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

public class RateLimitingStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final Quota quota;
    private final QuotaAction action;
    private final RequestCounter requestCounter;

    private volatile boolean enforceLimit;
    private volatile boolean softLimitExceeded;
    private volatile boolean hardLimitExceeded;

    public RateLimitingStore(Store<K, V, T> innerStore,
                             Quota quota,
                             QuotaAction action,
                             int durationMS) {
        super(innerStore);
        this.quota = quota;
        this.action = action;
        this.requestCounter = new RequestCounter(durationMS);
        this.enforceLimit = true;
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            return getInnerStore().get(key, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    private void enforceHardLimit() {
        if(enforceLimit && hardLimitExceeded)
            throw new RateLimitExceededException("Rate limit exceeded");
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            return getInnerStore().getAll(keys, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            getInnerStore().put(key, value, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            return getInnerStore().delete(key, version);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public void setEnforceLimit(boolean enforceLimit) {
        this.enforceLimit = enforceLimit;
    }

    public boolean isLimitEnforced() {
        return enforceLimit;
    }

    public void verifyLimits() {
        long throughput = (long) getThroughput();

        if(throughput > quota.getSoftLimit()) {
            if(!softLimitExceeded) {
                action.softLimitExceeded();
                softLimitExceeded = true;
            }
        } else {
            if(softLimitExceeded) {
                action.softLimitCleared();
                softLimitExceeded = false;
            }
        }

        if(throughput > quota.getHardLimit()) {
            if(!hardLimitExceeded) {
                action.hardLimitExceeded();
                hardLimitExceeded = true;
            }
        } else {
            if(hardLimitExceeded) {
                action.softLimitCleared();
                hardLimitExceeded = false;
            }
        }
    }

    @JmxGetter(name = "Throughput", description = "Throughput for all operations")
    public float getThroughput() {
        return requestCounter.getThroughput();
    }
}
