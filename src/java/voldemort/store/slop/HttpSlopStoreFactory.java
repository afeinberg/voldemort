package voldemort.store.slop;

import org.apache.commons.httpclient.HttpClient;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.utils.ByteArray;

/**
 * A {@link SlopStoreFactory} implementation using {@link HttpStore} for the
 * underlying transport.
 */
public class HttpSlopStoreFactory extends AbstractCachingSlopStoreFactory {

    private final HttpClient httpClient;
    private final Cluster cluster;
    private final String storeName;
    private final RequestFormatType requestFormatType;

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

    public HttpSlopStoreFactory(HttpClient httpClient,
                                RequestFormatType requestFormatType,
                                Cluster cluster,
                                String storeName) {
        this.httpClient = httpClient;
        this.cluster = cluster;
        this.storeName = storeName;
        this.requestFormatType = requestFormatType;
    }

    @Override
    protected Store<ByteArray, byte[]> getSlopStore(int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        return new HttpStore(storeName,
                             node.getHost(),
                             node.getHttpPort(),
                             httpClient,
                             requestFormatFactory.getRequestFormat(requestFormatType),
                             false);
    }
}
