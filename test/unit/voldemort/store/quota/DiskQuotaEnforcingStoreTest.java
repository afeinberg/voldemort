package voldemort.store.quota;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import voldemort.store.StorageEngine;
import voldemort.versioning.Versioned;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DiskQuotaEnforcingStoreTest {

    @Mock
    private StorageEngine<String, String, Void> mockStorageEngine;

    private volatile long diskUtilization = 0;
    private volatile boolean softLimitExceeded = false;
    private volatile boolean hardLimitExceeded = false;

    private QuotaAction quotaAction = new QuotaAction() {
        public void onSoftLimitExceeded() {
            softLimitExceeded = true;
        }

        public void onHardLimitExceeded() {
            hardLimitExceeded = true;
        }

        public void onSoftLimitCleared() {
            softLimitExceeded = false;
        }

        public void onHardLimitCleared() {
            hardLimitExceeded = false;
        }
    };

    private Quota quota = new Quota(100, 200);
    private DiskQuotaEnforcingStore<String, String, Void> quotaStore;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(mockStorageEngine.diskUtilization()).thenAnswer(new Answer<Long>() {

            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                return diskUtilization;
            }
        });

        quotaStore =  new DiskQuotaEnforcingStore<String, String, Void>(mockStorageEngine,
                                                                        quotaAction,
                                                                        quota);
    }

    @Test
    public void testNormalPut() {
        // Puts should go through without any issues before a hard or soft limit
        // is exceeded
        quotaStore.put("a", new Versioned<String>("1"), null);
        verify(mockStorageEngine, times(1)).put("a", new Versioned<String>("1"), null);
    }

    @Test
    public void testSoftLimit() {
        diskUtilization = 101;
        quotaStore.verifyQuota();

        assertTrue("soft limit exceeded", softLimitExceeded);

        // Puts should still go through when a soft limit is exceeded
        quotaStore.put("b", new Versioned<String>("1"), null);
        verify(mockStorageEngine, times(1)).put("b", new Versioned<String>("1"), null);

    }

    @Test
    public void testHardLimit() {
        diskUtilization = 201;
        quotaStore.verifyQuota();

        assertTrue("hard limit exceeded", hardLimitExceeded);

        // Puts should throw an exception when hard limit is exceeded
        boolean exceptionThrown = false;
        try {
            quotaStore.put("c", new Versioned<String>("1"), null);
        } catch(DiskQuotaExceededException e) {
            exceptionThrown = true;
        }

        assertTrue("exception thrown when hard limit exceeded", exceptionThrown);
    }

    @Test
    public void testRecover() {
        diskUtilization = 201;
        quotaStore.verifyQuota();
        assertTrue(softLimitExceeded);
        assertTrue(hardLimitExceeded);

        diskUtilization = 90;
        quotaStore.verifyQuota();
        assertFalse("hard limit cleared", hardLimitExceeded);
        assertFalse("soft limit cleared", softLimitExceeded);

        boolean exceptionThrown = false;
        try {
            quotaStore.put("c", new Versioned<String>("1"), null);
        } catch(DiskQuotaExceededException e) {
            exceptionThrown = true;
        }

        assertFalse("exception not thrown after limits cleared", exceptionThrown);
    }

    @Test
    public void testDisableEnforcement() {
        diskUtilization = 201;
        quotaStore.verifyQuota();
        assertTrue(softLimitExceeded);
        assertTrue(hardLimitExceeded);

        quotaStore.setEnforceQuota(false);
        boolean exceptionThrown = false;
        try {
            quotaStore.put("c", new Versioned<String>("1"), null);
        } catch(DiskQuotaExceededException e) {
            exceptionThrown = true;
        }

        assertFalse("exception not thrown when enforcement disabled", exceptionThrown);

        quotaStore.setEnforceQuota(true);
        exceptionThrown = false;
        try {
            quotaStore.put("c", new Versioned<String>("1"), null);
        } catch(DiskQuotaExceededException e) {
            exceptionThrown = true;
        }

        assertTrue("exception thrown after enforcement enabled again", exceptionThrown);
    }
}
