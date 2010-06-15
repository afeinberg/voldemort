package voldemort.store.slop;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.store.Store;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Caching slop store factory implementation for Http and Socket stores
 */
public abstract class AbstractCachingSlopStoreFactory implements SlopStoreFactory {
    private final Logger logger = Logger.getLogger(AbstractCachingSlopStoreFactory.class); 
    private final Serializer<ByteArray> keySerializer = new ByteArraySerializer();
    private final Serializer<Slop> valueSerializer = new SlopSerializer();
    
    private final ConcurrentMap<Integer, Future<Store<ByteArray, byte[]>>> cache =
                   new ConcurrentHashMap<Integer, Future<Store<ByteArray, byte[]>>>();

    /**
     * Return a cached slop store for a specific node in the cluster.
     * If there is no slop store cached for the current node, creates one. Uses a
     * {@link ConcurrentHashMap} for a thread safe cache.
     *
     * @param node Id of the node holding the store
     * @return Slop store for node
     */
    public Store<ByteArray, Slop> create(final int node) {
        Callable<Store<ByteArray, byte[]>> eval = new Callable<Store<ByteArray, byte[]>>() {
            public Store<ByteArray, byte[]> call() {
                return getSlopStore(node);
            }
        };
        FutureTask<Store<ByteArray, byte[]>> ft = new FutureTask<Store<ByteArray, byte[]>>(eval);
        Future<Store<ByteArray, byte[]>> f = cache.putIfAbsent(node, ft);
        if (f == null) {
            f = ft;
            ft.run();
        }
        try {
            return SerializingStore.wrap(f.get(),
                                         keySerializer,
                                         valueSerializer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VoldemortException(e);
        } catch (CancellationException e) {
            throw new VoldemortException(e);
        } catch (ExecutionException e) {
            throw new VoldemortException(e.getCause());
        }
    }

    public void close() {
        for (Map.Entry<Integer, Future<Store<ByteArray, byte[]>>> nodeStore: cache.entrySet()) {
            try {
                Future<Store<ByteArray, byte[]>> f = nodeStore.getValue();
                Store<ByteArray, byte[]> s = f.get();
                s.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error when closing slop at " + nodeStore.getKey(), e);
            }
        }
    }
        
    protected abstract Store<ByteArray, byte[]> getSlopStore(int node);
}
