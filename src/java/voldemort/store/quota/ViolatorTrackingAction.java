package voldemort.store.quota;

public class ViolatorTrackingAction implements QuotaAction {

    private final QuotaStatusJmx quotaStatusJmx;
    private final String storeName;

    public ViolatorTrackingAction(QuotaStatusJmx quotaStatusJmx,
                                  String storeName) {
        this.quotaStatusJmx = quotaStatusJmx;
        this.storeName = storeName;
    }

    public void onSoftLimitExceeded() {
        quotaStatusJmx.addSoftLimitViolator(storeName);
    }

    public void onHardLimitExceeded() {
        quotaStatusJmx.addHardLimitViolator(storeName);
    }

    public void onSoftLimitCleared() {
        quotaStatusJmx.removeSoftLimitViolator(storeName);
    }

    public void onHardLimitCleared() {
        quotaStatusJmx.removeHardLimitViolator(storeName);
    }
}
