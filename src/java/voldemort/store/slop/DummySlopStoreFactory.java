package voldemort.store.slop;

import voldemort.store.Store;
import voldemort.utils.ByteArray;

public class DummySlopStoreFactory implements SlopStoreFactory {

    public Store<ByteArray, Slop> create(int node) {
        throw new UnsupportedOperationException("This is a dummy store");
    }

    public void close() {

    }
}
