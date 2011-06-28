package voldemort.store.quota;


import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.DelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class DiskQuotaEnforcingStore<K, V, T>
        extends DelegatingStore<K, V, T>
        implements StorageEngine<K, V, T> {

    private static final Logger logger = Logger.getLogger(DiskQuotaEnforcingStore.class);

    private final StorageEngine<K, V, T> innerStorageEngine;
    private final QuotaAction action;
    private final Quota quota;
    private volatile boolean enforceQuota;

    protected boolean wasSoftLimitExceeded;
    protected boolean wasHardLimitExceeded;

    private volatile long diskUtilization;

    public DiskQuotaEnforcingStore(StorageEngine<K, V, T> innerStorageEngine,
                                   QuotaAction action,
                                   Quota quota) {
        super(innerStorageEngine);
        this.innerStorageEngine = innerStorageEngine;
        this.action = action;
        this.quota = quota;
        wasSoftLimitExceeded = false;
        wasHardLimitExceeded = false;
        enforceQuota = true;
        diskUtilization = 0;
    }

    @JmxGetter(name = "DiskUtilization", description = "Disk utilization in bytes")
    public long getDiskUtilization() {
        return diskUtilization;
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        if(isQuotaEnforced() && isHardLimitExceeded())
            throw new DiskQuotaExceededException("Hard limit is " + getQuota().getHardLimit()
                                               + " current utilization is " + diskUtilization);

        getInnerStore().put(key, value, transform);
    }

    public boolean isSoftLimitExceeded() {
        return diskUtilization > getQuota().getSoftLimit();
    }

    public boolean isHardLimitExceeded() {
        return diskUtilization > getQuota().getHardLimit();
    }

    public void computeQuotas() {
        diskUtilization = diskUtilization();
        logger.info("Computed disk utilization: " + diskUtilization);
    }

     @JmxGetter(name = "HardLimit", description = "Hard quota")
    public long getHardLimit() {
        return getQuota().getHardLimit();
    }

    @JmxGetter(name = "SoftLimit", description = "Soft quota")
    public long getSoftLimit() {
        return getQuota().getSoftLimit();
    }

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
                action.hardLimitExceeded();
            } else if(wasHardLimitExceeded) {
                wasHardLimitExceeded = false;
                action.hardLimitCleared();
            }

            if(isSoftLimitExceeded()) {
                wasSoftLimitExceeded = true;
                action.softLimitExceeded();
            } else if(wasSoftLimitExceeded) {
                wasSoftLimitExceeded = false;
                action.softLimitCleared();
            }
        }
    }

    /**
     * Are quotas enforced?
     *
     * @return True if quotas are enforced
     */
    @JmxGetter(name = "QuotaEnforce", description = "Is the quota enforced?")
    public boolean isQuotaEnforced() {
        return enforceQuota;
    }

    /**
     * Disable or enable quota enforcement. If set to false, quotas
     * won't be enforced and quota computation won't take place
     *
     * @param enforceQuota Should quotas be enforced
     */
    @JmxOperation(description = "Enable or disable quota enforcement")
    public void setEnforceQuota(boolean enforceQuota) {
        this.enforceQuota = enforceQuota;
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

    public boolean isPartitionAware() {
        return innerStorageEngine.isPartitionAware();
    }

}
