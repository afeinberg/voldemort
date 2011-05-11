package voldemort.store.quota;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import java.io.IOException;
import java.util.Properties;

public class DiskQuotaIntegrationTest {

    private final static String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";

    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private VoldemortServer server;
    private Cluster cluster;

    @Before
    public void setUp() throws IOException {
        VoldemortConfig config =  ServerTestUtils.createServerConfig(false,
                                                                     0,
                                                                     TestUtils.createTempDir()
                                                                              .getAbsolutePath(),
                                                                     null,
                                                                     STORES_XML_FILE,
                                                                     new Properties());
        config.setEnableQuota(true);
        config.setEnforceQuota(true);
        config.setQuotaVerificationFrequencyMs(10000);
        cluster = ServerTestUtils.getLocalCluster(1, new int [][] { { 0 } });
        server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                      config,
                                                      cluster);
    }

    @After
    public void shutDown() throws IOException {
        ServerTestUtils.stopVoldemortServer(server);
        socketStoreFactory.close();
    }

    @Test
    public void testNormalState() {

    }

    @Test
    public void testSoftLimitViolation() {

    }

    @Test
    public void testHardLimitViolation() {

    }

    @Test
    public void testRecovery() {

    }

}
