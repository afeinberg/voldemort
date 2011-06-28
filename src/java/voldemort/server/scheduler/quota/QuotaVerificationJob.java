package voldemort.server.scheduler.quota;


import org.apache.log4j.Logger;
import voldemort.store.quota.DiskQuotaEnforcingStore;

public class QuotaVerificationJob<K, V, T> implements Runnable {

    private final static Logger logger = Logger.getLogger(QuotaVerificationJob.class);

    private final DiskQuotaEnforcingStore<K, V, T> store;
    private final String description;

    public QuotaVerificationJob(DiskQuotaEnforcingStore<K, V, T> store,
                                String description) {
        this.store = store;
        this.description = description;
    }

    public void run() {
        logger.info("Starting " + description + " quota verification on store \""
                    + store.getName() + "\"...");
        try {
            store.verifyQuota();
        } catch(Exception e) {
            logger.error("Error running verification job", e);
        }
        logger.info("Quota verification finished on store \"" + store.getName() + "\".");
    }
}
