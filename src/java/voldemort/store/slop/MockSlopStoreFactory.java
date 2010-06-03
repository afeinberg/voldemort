package voldemort.store.slop;

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

    private final Map<Integer, Store<ByteArray, byte[]>> underlyingStores;

    private final Serializer<ByteArray> keySerializer = new ByteArraySerializer();
    private final Serializer<Slop> valueSerializer = new SlopSerializer();

    public MockSlopStoreFactory(Map<Integer, Store<ByteArray, byte[]>> underlyingStores) {
        this.underlyingStores = underlyingStores;
    }

    public Store<ByteArray, Slop> create(int node) {
        return SerializingStore.wrap(underlyingStores.get(node),
                                     keySerializer,
                                     valueSerializer);
    }

    public void close() {

    }
}
