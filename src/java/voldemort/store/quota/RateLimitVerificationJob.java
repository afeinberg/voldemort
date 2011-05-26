package voldemort.store.quota;

import org.apache.log4j.Logger;

public class RateLimitVerificationJob implements Runnable {

    private static final Logger logger = Logger.getLogger(RateLimitVerificationJob.class);

    private final RateLimitingStore rateLimitingStore;
    private final int intervalMS;
    private volatile boolean isRunning;

    public RateLimitVerificationJob(RateLimitingStore rateLimitingStore,
                                    int intervalMS) {
        this.rateLimitingStore = rateLimitingStore;
        this.intervalMS = intervalMS;
        this.isRunning = true;
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted() && isRunning) {
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("Sleeping for " + intervalMS
                                 + " ms before checking rate limits");
                }

                Thread.sleep(intervalMS);
            } catch(InterruptedException e) {
                break;
            }

            if(logger.isDebugEnabled())
                logger.debug("Verifying rate limit for " + rateLimitingStore.getName());

            rateLimitingStore.verifyLimits();

            if(logger.isDebugEnabled())
                logger.debug("Rate limit verified for " + rateLimitingStore.getName());
        }
    }

    public void close() {
        isRunning = false;
    }
}
