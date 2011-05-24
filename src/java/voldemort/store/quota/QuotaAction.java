package voldemort.store.quota;

public interface QuotaAction {

    /**
     * Invoked each time a soft limit is exceeded
     */
    public void softLimitExceeded();

    /**
     * Invoked each time a hard limit is exceeded
     */
    public void hardLimitExceeded();

    /**
     * Invoked when a soft limit was exceeded before, but no longer is
     */
    public void softLimitCleared();

    /**
     * Invoked when a hard limit was exceeded before, but no longer is
     */
    public void hardLimitCleared();
}
