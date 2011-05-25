package voldemort.store.quota;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.versioning.Versioned;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RateLimitingStoreTest {

    private static final Logger logger = Logger.getLogger(RateLimitingStoreTest.class);

    private final Quota quota = new Quota(100, 1000);

    @Mock
    private StorageEngine<String, String, Void> mockEngine;
    @Mock
    private QuotaAction action;

    private <K, V, T> RateLimitingStore<K, V, T> getLimitingStore(Store<K, V, T> store,
                                                                  Quota quota,
                                                                  QuotaAction action) {
        return new RateLimitingStore<K, V, T>(store, quota, action, 1000);
    }

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockEngine.get("foo", null)).thenReturn(Arrays.asList(Versioned.value("bar")));
    }

    @Test
    public void testGetThroughput() throws Exception {
        RateLimitingStore<String, String, Void> rlStore = getLimitingStore(mockEngine,
                                                                           quota,
                                                                           action);
        for(int i = 0; i < 10000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        assertTrue(rlStore.getThroughput() >= 1000);
    }

    @Test
    public void testSoftLimitViolation() {
        // TODO
    }

    @Test
    public void testHardLimitViolation() {
        // TODO
    }

    @Test
    public void testSoftLimitRecovery() {
        // TODO
    }

    @Test
    public void testHardLimitRecovery() {
        // TODO
    }
}
