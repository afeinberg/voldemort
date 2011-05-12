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
import voldemort.server.VoldemortService;
import voldemort.server.storage.StorageService;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class DiskQuotaIntegrationTest {

    private final static String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";
    private static final String STORE_NAME = "test-disk-quota";

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
            config.setEnableQuota(true);
            config.setEnforceQuota(true);
            config.setQuotaVerificationFrequencyMs(10000);
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
        assertEquals("put goes through succesfully", client.get("hello").getValue(), "world");
    }

    @Test
    public void testSoftLimitViolation() throws Exception {
        for(int i = 0; i < 500; i++) {
            client.put("k" + i, "v" + i);
        }
        Thread.sleep(10000);
        StorageService storageService = getStorageService();
        QuotaStatusJmx quotaStatusJmx = storageService.getDiskQuotaStatusJmx();
        assertTrue("soft limit violation caught",
                   quotaStatusJmx.getSoftLimitViolators().contains(STORE_NAME));
    }

    @Test
    public void testHardLimitViolation() {

    }

    @Test
    public void testRecovery() {

    }

    private StorageService getStorageService() {
        for(VoldemortService service: servers[0].getServices()) {
            if(service instanceof StorageService)
                return (StorageService) service;
        }
        throw new IllegalStateException("Storage service not found");
    }
}
