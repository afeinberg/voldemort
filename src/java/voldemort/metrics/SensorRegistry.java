package voldemort.metrics;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Threadsafe
public class SensorRegistry {

    // Using a non-concurrent map as all access to it will be synchronized
    private final Map<String, Object> sensors;
    private final Collection<SensorRegistryListener> listeners;

    public SensorRegistry(Collection<SensorRegistryListener> listeners) {
        this.listeners = listeners;
        sensors = new HashMap<String, Object>();
    }

    public void registerSensor(String domain, String type, Object object) {
        String objectName = createSensorName(domain, type);
        synchronized(this) {
            if(sensors.containsKey(objectName))
                unregisterSensor(domain, type);
            sensors.put(objectName, object);
        }

        for(SensorRegistryListener listener: listeners)
            listener.sensorRegistered(domain, type, object);
    }

    public void unregisterSensor(String domain, String type) {
        String objectName = createSensorName(domain, type);
        synchronized(this) {
            if(!sensors.containsKey(objectName))
                throw new VoldemortException("No sensor for domain "
                                             + domain
                                             + ", type "
                                             + type
                                             + " exists.");
            sensors.remove(objectName);
        }

        for(SensorRegistryListener listener: listeners)
            listener.sensorUnregistered(domain, type);
    }

    public synchronized Object getSensor(String domain, String type) {
        return sensors.get(createSensorName(domain, type));

    }

    private static String createSensorName(String domain, String type) {
        // Mimics the object names used by JMX
        return domain + ":type=" + type;
    }


}
