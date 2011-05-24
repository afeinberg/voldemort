package voldemort.store.quota;

import voldemort.VoldemortException;
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

    public RateLimitingStore(Store<K, V, T> innerStore,
                             Quota quota,
                             QuotaAction action,
                             int durationMS) {
        super(innerStore);
        this.quota = quota;
        this.action = action;
        this.requestCounter = new RequestCounter(durationMS);
    }

    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return getInnerStore().get(key, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return getInnerStore().getAll(keys, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            getInnerStore().put(key, value, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public boolean delete(K key, Version version) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return getInnerStore().delete(key, version);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    public float getThroughput() {
        return requestCounter.getThroughput();
    }
}
