package voldemort.performance;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.log4j.Logger;
import voldemort.TestUtils;
import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.bdb.BdbStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

import java.io.File;

/**
 * Micro-benchmark for the cleaner thread
 */
public class BdbCleanerStressTest {

    private static final Logger logger = Logger.getLogger(BdbCleanerStressTest.class);
    private static final Serializer<Object> keySer = new JsonTypeSerializer(JsonTypeDefinition.fromJson("'string'"));
    private static final Serializer<Object> valueSer = new JsonTypeSerializer(JsonTypeDefinition.fromJson("'string'"));

    private final BdbStorageEngine bdbEngine;
    private final int numRecords;
    private final int recordSize;

    public BdbCleanerStressTest(BdbStorageEngine bdbEngine,
                                int numRecords,
                                int recordSize) {
        this.bdbEngine = bdbEngine;
        this.numRecords = numRecords;
        this.recordSize = recordSize;
    }

    public void populateDatabase() {
        for(int i = 0; i < numRecords; i++) {
            ByteArray key = new ByteArray(keySer.toBytes(Integer.toString(i)));
            byte[] valueBytes = valueSer.toBytes(TestUtils.randomLetters(recordSize));
            bdbEngine.put(key, Versioned.value(valueBytes), null);
        }
    }

    public void benchmarkCleaner() {
        System.out.println("Populating the database: ");
        populateDatabase();
        System.out.println("Finished populating the database");

        System.out.println("Starting benchmark, writing over the values");
        long startSecs = System.nanoTime() / Time.NS_PER_SECOND;
        populateDatabase();
        long endSecs = System.nanoTime() / Time.NS_PER_SECOND;
        System.out.println("Writing over the values finished");

        try {
            Thread.sleep(1000 * 30);
        } catch(InterruptedException e) {
            logger.error(e);
            Thread.currentThread().interrupt();
        }

        long cleanerRuns = bdbEngine.getEnvironmentStats(true).getNCleanerRuns();
        long cleanedLNs = bdbEngine.getEnvironmentStats(true).getNLNsCleaned();
        long elapsedSecs = endSecs - startSecs;
        double cleanerRunsPerSec = elapsedSecs / cleanerRuns;
        double cleanedLNsPerSec = elapsedSecs / cleanedLNs;

        System.out.println("Took " + elapsedSecs + " seconds to re-populate Bdb");
        System.out.println("Performed " + cleanerRuns + " cleaner runs");
        System.out.println("Leaf nodes cleaned " + cleanedLNs);
        System.out.println("Cleaner runs / sec " + cleanerRunsPerSec);
        System.out.println("Leaf node cleaning throughput " + cleanedLNsPerSec);
    }

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("help", "print usage information");
            parser.accepts("records", "[REQUIRED] number of records")
                  .withRequiredArg()
                  .ofType(Integer.class);
            parser.accepts("record-length", "Record length in characters");
            parser.accepts("cleaner-threads", "Number of cleaner threads")
                  .withRequiredArg()
                  .ofType(Integer.class);
            parser.accepts("cleaner-buffer", "Cleaner buffer")
                  .withRequiredArg()
                  .ofType(Integer.class);
            parser.accepts("data-dir", "Directory for data")
                  .withRequiredArg()
                  .ofType(String.class);
            parser.accepts("clean-up", "Delete the data directory when done.");
            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                parser.printHelpOn(System.out);
                System.exit(-1);
            }

            CmdUtils.croakIfMissing(parser, options, "records");

            int numRecords = (Integer) options.valueOf("records");
            int recordLength = CmdUtils.valueOf(options, "record-length", 1024);
            int numCleanerThreads = CmdUtils.valueOf(options, "cleaner-threads", 1);
            int cleanerBuffer = CmdUtils.valueOf(options, "cleaner-buffer", 8192);
            boolean doCleanUp = options.has("clean-up");

            File dataDir = null;
            if(options.has("data-dir"))
                dataDir = new File((String) options.valueOf("data-dir"));
            else
                dataDir = TestUtils.createTempDir();
            System.out.println("Data dir: " + dataDir);

            Props props = new Props();
            props.put("node.id", 0);
            props.put("data.directory", dataDir.getAbsolutePath());
            props.put("vodemort.home", System.getProperty("user.dir"));
            VoldemortConfig config = new VoldemortConfig(props);
            config.setBdbCleanerThreads(numCleanerThreads);
            config.setBdbCleanerLookAheadCacheSize(cleanerBuffer);

            BdbStorageConfiguration storageConf = new BdbStorageConfiguration(config);
            BdbStorageEngine bdbEngine = (BdbStorageEngine) storageConf.getStore("test");
            BdbCleanerStressTest bdbCleanerStressTest = new BdbCleanerStressTest(bdbEngine,
                                                                                 numRecords,
                                                                                 recordLength);
            bdbCleanerStressTest.benchmarkCleaner();

            bdbEngine.close();
            if(doCleanUp) {
                System.out.println("Trying to delete the data directory");
                if(!dataDir.delete())
                    System.err.println("Failed to delete the data directory!");
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
