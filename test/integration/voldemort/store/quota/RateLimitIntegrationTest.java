package voldemort.store.quota;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class RateLimitIntegrationTest {

    private final static String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";
    private static final String STORE_NAME = "test-rate-limit";


    @Before
    public void setUp() throws IOException {
        // TODO: Setup a two node cluster
    }

    @After
    public void shutDown() throws IOException {
        // TODO: Shutdown the cluster
    }

    @Test
    public void testNormalState() {
        // TODO: Test normal operation
    }

    @Test
    public void testSingleViolator() {
        // TODO: Test that a single violator is banned
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
