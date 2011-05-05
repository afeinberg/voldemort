package voldemort.store.quota;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import voldemort.TestUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class QuotaUtilsTest {

    private File tempDir;
    private long expectedSize;

    @Before
    public void setUp() throws IOException {
        tempDir = TestUtils.createTempDir();
        expectedSize = 4096 * 10;

        for(int i = 0; i < 10; i++) {
            File newFile = new File(tempDir, Integer.toString(i));
            FileWriter fileWriter = new FileWriter(newFile);
            BufferedWriter writer = new BufferedWriter(fileWriter);
            try {
                for(int j = 0; j < 4096; j++)
                    writer.append('a');
                writer.flush();
            } finally {
                try {
                    writer.close();
                } finally {
                    fileWriter.close();
                }
            }
        }
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(tempDir);
    }

    @Test
    public void testDiskUtilization() {
        assertEquals("Return the size of temporary directory",
                     QuotaUtils.diskUtilization(tempDir.getAbsolutePath()), expectedSize);
    }

    @Test
    public void testDiskUtilizationKnownFileSize() {
        assertEquals("Return the size of temporary directory given file size",
                     QuotaUtils.diskUtilization(tempDir.getAbsolutePath(), 4096),
                     expectedSize);
    }
}
