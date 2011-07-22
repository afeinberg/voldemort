package voldemort.metrics;

public interface SensorRegistryListener {

    public void sensorRegistered(String domain, String type, Object object);

    public void sensorUnregistered(String domain, String type);
}
