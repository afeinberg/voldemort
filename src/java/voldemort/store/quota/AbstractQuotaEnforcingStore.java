package voldemort.store.quota;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public abstract class AbstractQuotaEnforcingStore<K, V, T>
        extends DelegatingStore<K, V, T>
        implements StorageEngine<K, V, T> {

    private final StorageEngine<K, V, T> innerStorageEngine;
    private final QuotaAction action;
    private final Quota quota;
    private volatile boolean enforceQuota;

    protected boolean wasSoftLimitExceeded;
    protected boolean wasHardLimitExceeded;

    public AbstractQuotaEnforcingStore(StorageEngine<K, V, T> innerStorageEngine,
                                       QuotaAction action,
                                       Quota quota) {
        super(innerStorageEngine);
        this.innerStorageEngine = innerStorageEngine;
        this.action = action;
        this.quota = quota;
        wasSoftLimitExceeded = false;
        wasHardLimitExceeded = false;
        enforceQuota = true;
    }

    /**
     * Calculate whether soft limit is exceeded
     *
     * @return True if soft limit exceeded
     */
    public abstract boolean isSoftLimitExceeded();

    /**
     * Calculate whether hard limit is exceeded
     *
     * @return True if hard limit is exceeded
     */
    public abstract boolean isHardLimitExceeded();

    /**
     * Perform (periodically) any expensive calculation needed to determine
     * whether or not a quota was exceeded
     */
    public abstract void computeQuotas();


    /**
     * Verify quotas:
     * </p>
     * Invoke #computeQuotas, then check if limits were exceeded. If
     * limits are exceeded, fire off appropriate actions. If a limit has been
     * exceeded after the previous run, but is no longer exceeded, fire off an
     * an "all clear" action
     */
    public void verifyQuota() {
        if(isQuotaEnforced()) {
            computeQuotas();

            if(isHardLimitExceeded()) {
                wasHardLimitExceeded = true;
                action.onHardLimitExceeded();
            } else if(wasHardLimitExceeded) {
                wasHardLimitExceeded = false;
                action.onHardLimitCleared();
            }

            if(isSoftLimitExceeded()) {
                wasSoftLimitExceeded = true;
                action.onSoftLimitExceeded();
            } else if(wasSoftLimitExceeded) {
                wasSoftLimitExceeded = false;
                action.onSoftLimitCleared();
            }
        }
    }

    /**
     * Are quotas enforced?
     *
     * @return True if quotas are enforced
     */
    public boolean isQuotaEnforced() {
        return enforceQuota;
    }

    /**
     * Disable or enable quota enforcement. If set to false, quotas
     * won't be enforced and quota computation won't take place
     *
     * @param enforceQuota Should quotas be enforced
     */
    public void setEnforceQuota(boolean enforceQuota) {
        this.enforceQuota = enforceQuota;
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        if(isQuotaEnforced()) {
            if(isHardLimitExceeded())
                throw new QuotaExceededException("Quota " + quota + " exceeded");
        }

        getInnerStore().put(key, value, transform);
    }

    public StorageEngine<K, V, T> getInnerStorageEngine() {
        return innerStorageEngine;
    }

    public Quota getQuota() {
        return quota;
    }

    public long diskUtilization() {
        return innerStorageEngine.diskUtilization();
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return innerStorageEngine.entries();
    }

    public ClosableIterator<K> keys() {
        return innerStorageEngine.keys();
    }

    public void truncate() {
        innerStorageEngine.truncate();
    }
}
