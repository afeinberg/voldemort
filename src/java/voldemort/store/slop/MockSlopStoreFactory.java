package voldemort.store.slop;

import com.google.common.collect.Maps;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.store.Store;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;

import java.util.Map;

/**
 * Mock {@link SlopStoreFactory} implementation.
 */
public class MockSlopStoreFactory implements SlopStoreFactory {

    private final Map<Integer, Store<ByteArray, Slop>> underlyingStores;

    private final static Serializer<ByteArray> keySerializer = new ByteArraySerializer();
    private final static Serializer<Slop> valueSerializer = new SlopSerializer();

    public MockSlopStoreFactory(Map<Integer, Store<ByteArray, Slop>> underlyingStores) {
        this.underlyingStores = underlyingStores;
    }

    public static MockSlopStoreFactory createFromByteStores(Map<Integer, Store<ByteArray, byte[]>> byteStores) {
        Map<Integer, Store<ByteArray, Slop>> slopStores = Maps.newHashMap();
        for (Map.Entry<Integer, Store<ByteArray, byte[]>> entry: byteStores.entrySet()) {
            slopStores.put(entry.getKey(), SerializingStore.wrap(entry.getValue(),
                                                                 keySerializer,
                                                                 valueSerializer));
        }

        return new MockSlopStoreFactory(slopStores);
    }



    public Store<ByteArray, Slop> create(int node) {
        return underlyingStores.get(node);
    }

    public void close() {
    }
}
