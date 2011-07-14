package voldemort.store.quota;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class RateLimitIntegrationTest {

    private final static String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";
    private static final String STORE_NAME = "test-rate-limit";

    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private VoldemortServer servers[] = new VoldemortServer[2];
    private VoldemortConfig configs[] = new VoldemortConfig[2];
    private Cluster cluster;
    private StoreClient<String, String> client;

    @Before
    public void setUp() throws IOException {
        for(int i = 0; i < 2; i++) {
            configs[i] =  ServerTestUtils.createServerConfig(false,
                                                             i,
                                                             TestUtils.createTempDir()
                                                                      .getAbsolutePath(),
                                                             null,
                                                             STORES_XML_FILE,
                                                             new Properties());
        }
        for(VoldemortConfig config: configs) {
            config.setRateLimitVerificationFrequencyMs(500);
            config.setEnableQuota(true);
            config.setEnforceQuota(true);
        }

        cluster = ServerTestUtils.getLocalCluster(2, new int [][] { { 0, 2 },  { 1, 3 } });

        for(int i = 0; i < 2; i++) {
            servers[i] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                              configs[i],
                                                              cluster);
        }

        String bootstrapUrl = cluster.getNodeById(0).getSocketUrl().toString();
        ClientConfig config = new ClientConfig().setBootstrapUrls(bootstrapUrl);
        StoreClientFactory factory = new SocketStoreClientFactory(config);
        client = factory.getStoreClient(STORE_NAME);
    }

    @After
    public void shutDown() throws IOException {
        for(VoldemortServer server: servers) {
            try {
                ServerTestUtils.stopVoldemortServer(server);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        socketStoreFactory.close();
    }

    @Test
    public void testNormalState() {
        client.put("hello", "world");
        assertEquals(client.getValue("hello"), "world");
    }

    @Test
    public void testSingleViolator() {
        boolean caughtException = false;
        try {
            for(int i = 0; i < 2000; i++) {
                String str = Integer.toString(i);
                client.put(str, str);
            }
        } catch(RateLimitExceededException re) {
            caughtException = true;
        }

        assertTrue("caught a single violator", caughtException);
    }

    @Test
    public void testSingleViolatorRecovery() {
        // TODO: Test that a single violator recovers
    }

    @Test
    public void testMultipleStores() {
        // TODO: Test the impact of violator stores on non-violators
    }
}
