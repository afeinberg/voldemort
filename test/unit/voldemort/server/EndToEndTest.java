package voldemort.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * Provides an unmocked end to end unit test of a Voldemort cluster.
 * 
 */
@RunWith(Parameterized.class)
public class EndToEndTest {

    private static final String STORE_NAME = "test-readrepair-memory";
    private static final String STORES_XML = "test/common/voldemort/config/stores.xml";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private final boolean useNio;

    private List<VoldemortServer> servers;
    private Cluster cluster;
    private StoreClient<String, String> storeClient;

    public EndToEndTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 2, 4, 6 }, { 1, 3, 5, 7 } });
        servers = new ArrayList<VoldemortServer>();
        Properties serverProps = new Properties();
        serverProps.put("max.threads", "100");
        serverProps.put("bdb.fair.latches", "true");

        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                            0,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            serverProps),
                                                         cluster));
        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                            1,
                                                                                            TestUtils.createTempDir()
                                                                                                     .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            serverProps),
                                                         cluster));
        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        storeClient = storeClientFactory.getStoreClient(STORE_NAME);
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    /**
     * Test the basic get/getAll/put/delete functionality.
     */
    @Test
    public void testSanity() {
        storeClient.put("Belarus", "Minsk");
        storeClient.put("Russia", "Moscow");
        storeClient.put("Ukraine", "Kiev");
        storeClient.put("Kazakhstan", "Almaty");

        Versioned<String> v1 = storeClient.get("Belarus");
        assertEquals("get/put work as expected", "Minsk", v1.getValue());

        storeClient.put("Kazakhstan", "Astana");
        Versioned<String> v2 = storeClient.get("Kazakhstan");
        assertEquals("clobbering a value works as expected, we have read-your-writes consistency",
                     "Astana",
                     v2.getValue());

        Map<String, Versioned<String>> capitals = storeClient.getAll(Arrays.asList("Russia",
                                                                                   "Ukraine"));

        assertEquals("getAll works as expected", "Moscow", capitals.get("Russia").getValue());
        assertEquals("getAll works as expected", "Kiev", capitals.get("Ukraine").getValue());

        storeClient.delete("Ukraine");
        assertNull("delete works as expected", storeClient.get("Ukraine"));
    }

    @Test
    public void testConcurrentReadsAfterPuts() throws Exception {
        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        final StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                                                     .setEnablePipelineRoutedStore(true)
                                                                                                     .setSelectors(1));
        StoreClient<String, String> storeClient = storeClientFactory.getStoreClient("test-highvolume-persistent");
        storeClient.put("a", "0");

        ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicBoolean failed = new AtomicBoolean(false);
        for(int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {
                public void run() {
                    StoreClient<String, String> otherClient = storeClientFactory.getStoreClient("test-highvolume-persistent");
                    for(int i = 0; i < 1000 && !failed.get(); i++) {

                        Versioned<String> versioned = otherClient.get("a");
                        if(versioned == null)
                            failed.set(true);
                        else {
                            versioned.setObject(Integer.toString(Integer.parseInt(versioned.getValue()) + 1));
                            try {
                                otherClient.put("a", versioned);
                            } catch(ObsoleteVersionException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertFalse("there should not have been any nulls", failed.get());
    }
}
