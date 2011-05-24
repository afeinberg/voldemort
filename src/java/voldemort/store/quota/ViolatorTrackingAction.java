package voldemort.store.quota;

public class ViolatorTrackingAction implements QuotaAction {

    private final QuotaStatusJmx quotaStatusJmx;
    private final String storeName;

    public ViolatorTrackingAction(QuotaStatusJmx quotaStatusJmx,
                                  String storeName) {
        this.quotaStatusJmx = quotaStatusJmx;
        this.storeName = storeName;
    }

    public void softLimitExceeded() {
        quotaStatusJmx.addSoftLimitViolator(storeName);
    }

    public void hardLimitExceeded() {
        quotaStatusJmx.addHardLimitViolator(storeName);
    }

    public void softLimitCleared() {
        quotaStatusJmx.removeSoftLimitViolator(storeName);
    }

    public void hardLimitCleared() {
        quotaStatusJmx.removeHardLimitViolator(storeName);
    }
}
