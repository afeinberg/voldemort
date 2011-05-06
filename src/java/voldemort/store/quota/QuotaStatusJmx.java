package voldemort.store.quota;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@JmxManaged
@Threadsafe
public class QuotaStatusJmx {

    private final String description;
    private final Set<String> hardLimitViolators;
    private final Set<String> softLimitViolators;

    public QuotaStatusJmx(String description) {
        this.description = description;
        this.hardLimitViolators = new CopyOnWriteArraySet<String>();
        this.softLimitViolators = new CopyOnWriteArraySet<String>();
    }

    @JmxGetter(name = "Description", description = "Description of the quota")
    public String getDescription() {
        return description;
    }

    @JmxGetter(name = "HardLimitViolators", description = "List stores that violate the hard limit")
    public Collection<String> getHardLimitViolators() {
        return hardLimitViolators;
    }

    @JmxGetter(name = "SoftLimitViolators", description = "List stores that violate the soft limit")
    public Collection<String> getSoftLimitViolators() {
        return softLimitViolators;
    }

    public void removeHardLimitViolator(String storeName) {
        hardLimitViolators.remove(storeName);
    }

    public void removeSoftLimitViolator(String storeName) {
        softLimitViolators.remove(storeName);
    }

    public void addHardLimitViolator(String storeName) {
        hardLimitViolators.add(storeName);
    }
    public void addSoftLimitViolator(String storeName) {
        softLimitViolators.add(storeName);
    }
}
