package voldemort.store.slop;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.utils.ByteArray;

/**
 * A {@link SlopStoreFactory} implementation using {@link SocketStore} for
 * the underlying transport.
 */
public class SocketSlopStoreFactory implements SlopStoreFactory {
    
    private final SocketStoreFactory socketStoreFactory;
    private final Cluster cluster;
    private final String storeName;
    private final RequestFormatType requestFormatType;

    private final Serializer<ByteArray> keySerializer = new ByteArraySerializer();
    private final Serializer<Slop> valueSerializer = new SlopSerializer();

    /**
     * Creates <code>SocketSlopStoreFactory</code>
     *
     * @param socketStoreFactory The {@link SocketStoreFactory} instance to use
     * @param requestFormatType The {@link RequestFormatType} to use
     * @param cluster Current {@link Cluster}
     * @param storeName Name of the slop store
     */
    public SocketSlopStoreFactory(SocketStoreFactory socketStoreFactory,
                                  RequestFormatType requestFormatType,
                                  Cluster cluster,
                                  String storeName) {
        this.socketStoreFactory = socketStoreFactory;
        this.cluster = cluster;
        this.storeName = storeName;
        this.requestFormatType = requestFormatType;
    }
    
    public Store<ByteArray, Slop> create(int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        SocketStore socketStore = socketStoreFactory.create(storeName,
                                                            node.getHost(),
                                                            node.getSocketPort(),
                                                            requestFormatType,
                                                            RequestRoutingType.IGNORE_CHECKS);

        return SerializingStore.wrap(socketStore,
                                     keySerializer,
                                     valueSerializer);
    }

    public void close() {
        socketStoreFactory.close();
    }
}
