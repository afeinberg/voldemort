package voldemort.store.quota;

public interface QuotaAction {

    /**
     * Invoked each time a soft limit is exceeded
     */
    public void onSoftLimitExceeded();

    /**
     * Invoked each time a hard limit is exceeded
     */
    public void onHardLimitExceeded();

    /**
     * Invoked when a soft limit was exceeded before, but no longer is
     */
    public void onSoftLimitCleared();

    /**
     * Invoked whena hard limit was exceeded before, but no longer is
     */
    public void onHardLimitCleared();
}
