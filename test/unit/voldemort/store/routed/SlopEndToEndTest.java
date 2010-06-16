package voldemort.store.routed;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Unmocked end to end test of hinted handoff
 */
@RunWith(Parameterized.class)
public class SlopEndToEndTest {

    private static final Logger logger = Logger.getLogger(SlopEndToEndTest.class);
    private static final String STORE_NAME = "test-readrepair-memory";
    private static final String STORES_XML = "test/common/voldemort/config/stores.xml";

    private final boolean useNio;

    private List<VoldemortServer> servers;
    private Cluster cluster;
    private SocketStoreFactory socketStoreFactory;

    public SlopEndToEndTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        Properties props = new Properties();
        props.put("enable.slop", "true");
        props.put("slop.frequency.ms", "250");

        cluster = ServerTestUtils.getLocalCluster(3, new int [][] { {0, 3, 5 },
                                                                    {1, 4, 6 },
                                                                    {2, 7 } } );
        socketStoreFactory = new ClientRequestExecutorPool(2,
                                                           10000,
                                                           100000,
                                                           32 * 1024);

        for (int i = 0; i < 3; i++)
            servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                             ServerTestUtils.createServerConfig(useNio,
                                                                                                i,
                                                                                                TestUtils.createTempDir()
                                                                                                               .getAbsolutePath(),
                                                                                                null,
                                                                                                STORES_XML,
                                                                                                props),
                                                             cluster));
    }

    @After
    public void tearDown() {
        for (VoldemortServer server: servers) {
            try {
                server.stop();
            } catch (Exception e) {
                logger.warn(e, e);
            }
        }
        socketStoreFactory.close();
    }

    @Test
    public void testSlop() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setEnablePipelineRoutedStore(true);
        config.setEnableHintedHandoff(true);

        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(config);
        StoreClient<String, String> storeClient = storeClientFactory.getStoreClient(STORE_NAME);
    }
        


}
