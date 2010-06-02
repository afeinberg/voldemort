package voldemort.store.slop;

import voldemort.store.Store;
import voldemort.utils.ByteArray;


public interface SlopStoreFactory {

    /**
     * Get a slop store located on the given node id.
     *
     * @param node Id of the node holding the store
     * @return The appropriate slop store
     */
    public Store<ByteArray, Slop> create(int node);

    /**
     * Close the store client(s) 
     */
    public void close();
}
