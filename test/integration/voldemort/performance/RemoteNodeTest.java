package voldemort.performance;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.*;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author afeinberg
 */
public class RemoteNodeTest {

    public static final int MAX_WORKERS = 8;

    public static void main(String [] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("requests", "[REQUIRED] number of requests")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("num-requests");
        parser.accepts("url", "[REQUIRED] bootstrap-url")
                       .withRequiredArg();
        parser.accepts("stores", "[REQUIRED] list of stores")
                       .withRequiredArg()
                       .withValuesSeparatedBy(',')
                       .describedAs("store names");
        parser.accepts("input", "[REQUIRED] file containing list of keys")
                       .withRequiredArg()
                       .describedAs("input-file");
        parser.accepts("threads", "max number of concurrent worker threads Default " + MAX_WORKERS)
                       .withRequiredArg()
                       .ofType(Integer.class);
        
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "url",
                                               "stores",
                                               "requests",
                                               "input");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String bootstrapUrl = (String) options.valueOf("url");
        String inputFile = (String) options.valueOf("input");

        int numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        int numRequests = (Integer) options.valueOf("requests");

        @SuppressWarnings("unchecked")
        List<String> stores = (List<String>) options.valuesOf("stores");

        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setMaxThreads(numThreads)
                       .setMaxTotalConnections(numThreads)
                       .setMaxConnectionsPerNode(numThreads)
                       .setBootstrapUrls(bootstrapUrl));

        ImmutableList.Builder<StoreClient<Integer,byte[]>> builder = ImmutableList.builder();
        for (String storeName: stores) {
            builder.add(factory.<Integer, byte[]>getStoreClient(storeName));
        }

        long start = System.currentTimeMillis();
        final List<StoreClient<Integer,byte[]>> storeClients = builder.build();
        final AtomicInteger nulls = new AtomicInteger(0);
        final AtomicInteger writes = new AtomicInteger(0);
        int lines = 0;
        final Random random = new Random(92873498274L);
        final CountDownLatch countDownLatch = new CountDownLatch(numRequests);
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(inputFile));
            String line = null;
            while ((line = in.readLine()) != null) {
                lines++;
                final int key = Integer.valueOf(line.replaceAll("\\s+", ""));
                service.execute(new Runnable() {
                    public void run() {
                        try {
                            int storeNum;
                            synchronized(random) {
                                storeNum = random.nextInt(storeClients.size());
                            }
                            StoreClient<Integer,byte[]> store = storeClients.get(storeNum);
                           
                            store.applyUpdate(new UpdateAction<Integer,byte[]>() {
                                @Override
                                public void update(StoreClient<Integer,byte[]> client) {
                                    Versioned<byte[]> value = client.get(key);
                                    if (value != null) {
                                        client.put(key, value);
                                        writes.incrementAndGet();
                                    } else
                                        nulls.incrementAndGet();
                                }
                            });
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
            }
        } catch (FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
        finally {
            factory.close();
            if (in != null)
                in.close();
        }

        // if more keys requested than are in file
        if (lines < numRequests) {
            for (int i = lines; i < numRequests; i++) {
                countDownLatch.countDown();
            }
        }
        countDownLatch.await();

        long totalTime = System.currentTimeMillis() - start;

        System.out.println("Lines read: " + lines);
        System.out.println("Null values encountered: " + nulls.get());
        System.out.println("Writes performed: " + writes.get());
        System.out.println("Throughput: " + (numRequests / (float) totalTime * 1000) + " keys/sec.");

        service.shutdown();
    }
}
