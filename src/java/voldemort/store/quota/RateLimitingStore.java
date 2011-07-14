package voldemort.store.quota;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

    private static final Logger logger = Logger.getLogger(RateLimitingStore.class);

    private final Quota quota;
    private final QuotaAction action;
    private final RequestCounter requestCounter;
    private final int durationMS;
    private final long verificationFrequencyMS;
    private final long bannageIntervalMS;

    private volatile RateLimitVerificationJob verificationJob;

    private volatile boolean enforceLimit;
    private volatile boolean softLimitExceeded;
    private volatile boolean hardLimitExceeded;
    private volatile long bannageStartedMS;

    public RateLimitingStore(Store<K, V, T> innerStore,
                             Quota quota,
                             QuotaAction action,
                             int durationMS,
                             long verificationFrequencyMS,
                             int bannageIntervalMS) {
        super(innerStore);
        this.quota = quota;
        this.action = action;
        this.requestCounter = new RequestCounter(durationMS);
        this.durationMS = durationMS;
        this.verificationJob = null;
        this.softLimitExceeded = false;
        this.hardLimitExceeded = false;
        this.enforceLimit = true;
        this.bannageIntervalMS = bannageIntervalMS;
        this.verificationFrequencyMS = verificationFrequencyMS;
        init();
    }

    public final void init()  {
        verificationJob = new RateLimitVerificationJob(this, verificationFrequencyMS);

        Thread verificationThread = new Thread(verificationJob,
                                               "RateLimitVerification");
        verificationThread.setDaemon(true);
        verificationThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread t, Throwable e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error("Uncaught exception in rate limit verification thread:", e);
            }
        });

        verificationThread.start();
    }

    @Override
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

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            return getInnerStore().getAll(keys, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    @Override
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            getInnerStore().put(key, value, transforms);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        long start = System.nanoTime();
        try {
            enforceHardLimit();
            return getInnerStore().delete(key, version);
        } finally {
            requestCounter.addRequest(System.nanoTime() - start);
        }

    }

    @Override
    public synchronized void close() throws VoldemortException {
        if(verificationJob != null)
            verificationJob.close();
        else {
            if(logger.isEnabledFor(Level.ERROR))
                logger.warn("Warning closing RateLimitingStore "
                            + getName()
                            + ": verification job is not initialized!");
        }

        getInnerStore().close();
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
                bannageStartedMS = System.currentTimeMillis();
            }
        } else {
            if(hardLimitExceeded) {
                if(System.currentTimeMillis() - bannageStartedMS > bannageIntervalMS) {
                    action.hardLimitCleared();
                    hardLimitExceeded = false;
                }
            }
        }
    }

    @JmxGetter(name = "Throughput", description = "Throughput for all operations")
    public float getThroughput() {
        return requestCounter.getThroughput();
    }
}
