package voldemort.store.slop;

import voldemort.client.StoreClientFactory;
import voldemort.store.Store;
import voldemort.utils.ByteArray;


public abstract class AbstractSlopStoreFactory<SCF extends StoreClientFactory>
               implements SlopStoreFactory {

    protected final SCF storeClientFactory;

    /**
     * Constructs the <code>AbstractSlopStoreFactory</code>
     *
     * @param storeClientFactory The {@link StoreClientFactory} that should be
     *        used to route to specific nodes.
     */
    public AbstractSlopStoreFactory(SCF storeClientFactory) {
        this.storeClientFactory = storeClientFactory;
    }

   
    public abstract Store<ByteArray, Slop> create(int node);

    public void close() {
        storeClientFactory.close();
    }
}
