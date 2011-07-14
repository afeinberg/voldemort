package voldemort.store.quota;

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

    private final Quota quota = new Quota(100, 1000);

    @Mock
    private StorageEngine<String, String, Void> mockEngine;
    @Mock
    private QuotaAction action;
    private RateLimitingStore<String, String, Void> rlStore;

    private <K, V, T> RateLimitingStore<K, V, T> getLimitingStore(Store<K, V, T> store,
                                                                  Quota quota,
                                                                  QuotaAction action) {
        return new RateLimitingStore<K, V, T>(store, quota, action, 1000, 1000, 1000);
    }

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockEngine.get("foo", null)).thenReturn(Arrays.asList(Versioned.value("bar")));
        rlStore = getLimitingStore(mockEngine,
                                   quota,
                                   action);
    }

    @Test
    public void testGetThroughput() {
        for(int i = 0; i < 2000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        assertTrue("getThroughput works as expected", rlStore.getThroughput() >= 1000);
    }

    @Test
    public void testSoftLimitViolation() {
        for(int i = 0; i < 200; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        rlStore.verifyLimits();
        verify(action).softLimitExceeded();
    }

    @Test
    public void testHardLimitViolation() {
        for(int i = 0; i < 2000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        rlStore.verifyLimits();

        boolean exceptionCaught = false;
        try {
            rlStore.put("foo", Versioned.value("bar"), null);
        } catch(RateLimitExceededException e) {
            exceptionCaught = true;
        }

        assertTrue("RateLimitExceededException thrown", exceptionCaught);
        verify(action).hardLimitExceeded();
    }

    @Test
    public void testHardLimitBanAfterViolation() throws Exception {
        for(int i = 0; i < 2000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        rlStore.verifyLimits();

        boolean exceptionCaught = false;
        try {
            rlStore.put("foo", Versioned.value("bar"), null);
        } catch(RateLimitExceededException e) {
            exceptionCaught = true;
        }
        assertTrue("RateLimitExceededException thrown", exceptionCaught);
        verify(action).hardLimitExceeded();

        exceptionCaught = false;
        try {
            Thread.sleep(100);
            rlStore.put("a", Versioned.value("b"), null);
        } catch(RateLimitExceededException e) {
            exceptionCaught = true;
        }
        assertTrue("RateLimitExceededException still thrown after 100 ms", exceptionCaught);

        exceptionCaught = false;
        try {
            Thread.sleep(2000);
            rlStore.put("a", Versioned.value("b"), null);
        } catch(RateLimitExceededException e) {
            exceptionCaught = true;
        }
        assertFalse("RateLimitExceededException no longer thrown after 2000 ms", exceptionCaught);
    }

    @Test
    public void testSoftLimitRecovery() throws Exception {
        for(int i = 0; i < 2000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        rlStore.verifyLimits();

        verify(action).softLimitExceeded();

        Thread.sleep(2000);
        rlStore.verifyLimits();

        verify(action).softLimitCleared();
    }

    @Test
    public void testHardLimitRecovery() throws Exception {
        for(int i = 0; i < 2000; i++)
            rlStore.put("foo", Versioned.value("bar"), null);
        rlStore.verifyLimits();

        verify(action).hardLimitExceeded();

        Thread.sleep(2000);
        rlStore.verifyLimits();

        verify(action).hardLimitCleared();

        boolean exceptionCaught = false;
        try {
            rlStore.put("foo", Versioned.value("bar"), null);
        } catch(RateLimitExceededException e) {
            exceptionCaught = true;
        }

        assertFalse("recovered from hard limit violation", exceptionCaught);
    }
}
