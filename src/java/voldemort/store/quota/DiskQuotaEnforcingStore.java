package voldemort.store.quota;


import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.StorageEngine;
import voldemort.versioning.Versioned;

public class DiskQuotaEnforcingStore<K, V, T> extends AbstractQuotaEnforcingStore<K, V, T> {

    private static final Logger logger = Logger.getLogger(DiskQuotaEnforcingStore.class);

    private volatile long diskUtilization;

    public DiskQuotaEnforcingStore(StorageEngine<K, V, T> innerStorageEngine,
                                   QuotaAction action,
                                   Quota quota) {
        super(innerStorageEngine, action, quota);
        this.diskUtilization = 0;
    }

    @JmxGetter(name = "DiskUtilization", description = "Disk utilization in bytes")
    public long getDiskUtilization() {
        return diskUtilization;
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        if(isQuotaEnforced() && isHardLimitExceeded())
            throw new DiskQuotaExceedException("Hard limit is " + getQuota().getHardLimit()
                                               + " current utilization is " + diskUtilization);

        getInnerStore().put(key, value, transform);
    }

    @Override
    public boolean isSoftLimitExceeded() {
        return diskUtilization > getQuota().getSoftLimit();
    }

    @Override
    public boolean isHardLimitExceeded() {
        return diskUtilization > getQuota().getHardLimit();
    }

    @Override
    public void computeQuotas() {
        diskUtilization = diskUtilization();
        logger.info("Computed disk utilization: " + diskUtilization);
    }
}
