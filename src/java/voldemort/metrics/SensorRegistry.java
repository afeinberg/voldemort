package voldemort.metrics;

import com.google.common.collect.ImmutableSet;
import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.JmxUtils;
import voldemort.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Threadsafe
public class SensorRegistry {

    // Using a non-concurrent map as all access to it will be synchronized
    private final Map<Pair<String, String>, Object> sensors;
    private final Collection<SensorRegistryListener> listeners;

    public SensorRegistry(Collection<SensorRegistryListener> listeners) {
        this.listeners = listeners;
        sensors = new HashMap<Pair<String, String>, Object>();
    }

    public void registerSensor(String type, Object object) {
        registerSensor(JmxUtils.getPackageName(object.getClass()), type, object);
    }

    public void registerSensor(String domain, String type, Object object) {
        Pair<String, String> objectKey = Pair.create(domain, type);
        synchronized(this) {
            if(sensors.containsKey(objectKey))
                unregisterSensor(domain, type);
            sensors.put(objectKey, object);
        }

        for(SensorRegistryListener listener: listeners)
            listener.sensorRegistered(domain, type, object);
    }

    public void unregisterSensor(String domain, String type) {
        Pair<String, String> objectKey = Pair.create(domain, type);
        synchronized(this) {
            if(!sensors.containsKey(objectKey))
                throw new VoldemortException("No sensor for domain "
                                             + domain
                                             + ", type "
                                             + type
                                             + " exists.");
            sensors.remove(objectKey);
        }

        for(SensorRegistryListener listener: listeners)
            listener.sensorUnregistered(domain, type);
    }

    public synchronized Object getSensor(String domain, String type) {
        return sensors.get(Pair.create(domain, type));
    }

    public synchronized Collection<Pair<String, String>> getSensors() {
        return ImmutableSet.copyOf(sensors.keySet());
    }
}
