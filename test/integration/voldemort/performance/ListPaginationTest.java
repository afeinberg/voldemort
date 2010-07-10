package voldemort.performance;

import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test the performance of list pagination on two stores: one store containing
 * a mapping of keys to lists of guids, the other containing the mapping of
 * guids to values.
 */
public class ListPaginationTest {

    private final static int MAX_WORKERS = 8;
    private final static int MAX_TRIES = 32;
    private final static long MAX_MSG_GUID = 0x1l << 48;

    public interface KeyProvider<T> {
        public T next();
    }

    public abstract static class AbstractKeyProvider<T> implements KeyProvider<T> {
        private final List<Integer> keys;
        private final AtomicInteger index;

        private AbstractKeyProvider(int start, List<Integer> keys) {
            this.index = new AtomicInteger(start);
            this.keys = keys;
        }

        public Integer nextInteger() {
            if (keys != null)
                return keys.get(index.getAndIncrement()) % keys.size();
            else
                return index.getAndIncrement();
        }

        public abstract T next();
    }

    public static class IntegerKeyProvider extends AbstractKeyProvider<Integer> {
        private IntegerKeyProvider(int start, List<Integer> keys) {
            super(start, keys);
        }

        @Override
        public Integer next() {
            return nextInteger();
        }
    }

    public static class LongKeyProvider implements KeyProvider<Long> {
        private final AtomicLong index;
        private final Iterator<Long> iterator;

        private LongKeyProvider(long start, Collection<Long> keys) {
            this.index = new AtomicLong(start);
            this.iterator = keys == null ? null : keys.iterator();
        }

        public Long next() {
            if (iterator != null)
                return iterator.next();
            else
                return index.getAndIncrement();
        }
    }

    public static List<Integer> loadKeys(String path) throws IOException {

        List<Integer> targets = new ArrayList<Integer>();
        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null)
                targets.add(Integer.valueOf(text.replaceAll("\\s+", "")));
        } finally {
            try {
                if(reader != null)
                    reader.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        return targets;
    }

    public static <K, V> void appendToList(StoreClient<K,List<V>> indexStore,
                                           final K key,
                                           final V value) {
        indexStore.applyUpdate(new UpdateAction<K, List<V>> () {
            @Override
            public void update(StoreClient<K, List<V>> storeClient) {
                Versioned<List<V>> existing = storeClient.get(key);
                List<V> list;

                if (existing == null) {
                    list = Lists.newLinkedList();
                    existing = new Versioned<List<V>>(list);
                }
                else
                    list = existing.getValue();

                list.add(value);
                existing.setObject(list);
                storeClient.put(key, existing);
            }
        }, MAX_TRIES);
    }

    public static <K, V1, V2> void insertMessage(StoreClient<K, List<V1>> indexStore,
                                                 StoreClient<V1, V2> messageStore,
                                                 K pkId,
                                                 V1 msgId,
                                                 V2 message) {
        messageStore.put(msgId, message);
        appendToList(indexStore, pkId, msgId);
    }

    private static void printStatistics(String noun, int successes, long start, long[] requestTimes) {
        long queryTime = System.nanoTime() - start;
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(4);
        System.out.println("Throughput: " + (successes / (float) queryTime * Time.NS_PER_SECOND)
                           + " " + noun + "/sec.");
        System.out.println("Avg. " + noun + " latency: " + nf.format(TestUtils.mean(requestTimes))
                           + " ms.");
        System.out.println("95th percentile " + noun + " latency: "
                           + TestUtils.quantile(requestTimes, .95) + " ms.");
        System.out.println("99th percentile " + noun + " latency: "
                           + TestUtils.quantile(requestTimes, .99) + " ms.");
        System.out.println(successes + " successful " + noun + ".");
    }

    private static void printNulls(int nulls, long start) {
        long nullTime = System.nanoTime() - start;
        System.out.println((nulls / (float) nullTime * Time.NS_PER_SECOND) + " nulls/sec");
        System.out.println(nulls + " null values.");
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("r", "execute read operations");
        parser.accepts("w", "execute write operations");
        parser.accepts("m", "execute a mix of read and write requests");
        parser.accepts("v", "verbose");
        parser.accepts("ignore-nulls", "ignore null values");
        parser.accepts("request-file", "execute specific requests in order").withRequiredArg();
        parser.accepts("value-size", "size in bytes for random value. Default = 1024")
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("iterations", "number of times to repeat the test. Default = 1")
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("interval", "print results at this interval. Default = 1000 [0 to disable]")
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("threads", "max number of concurrent worker threads. Default = "
                                  + MAX_WORKERS)
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("help");

        OptionSet options = parser.parse(args);
        List<String> rest = options.nonOptionArguments();

        if (rest.size() != 4)
            System.exit(1);

        String url = rest.get(0);
        String indexStoreName = rest.get(1);
        String messageStoreName = rest.get(2);
        Integer numRequests = Integer.parseInt(rest.get(3));

        String ops = "";
        List<Integer> primaryKeys = null;

        Integer valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        Integer numIterations = CmdUtils.valueOf(options, "iterations", 1);
        Integer numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);

        final Integer interval = CmdUtils.valueOf(options, "interval", 1000);
        final boolean verbose = options.has("v");

        if (options.has("request-file"))
            primaryKeys = loadKeys((String) options.valueOf("request-file"));

        if (options.has("r"))
            ops += "r";
        if (options.has("w"))
            ops += "w";
        if (options.has("m"))
            ops += "m";
        if (ops.length() == 0)
            ops += "rw";

        System.out.println("operations : " + ops);
        System.out.println("value size : " + valueSize);
        System.out.println("iterations : " + numIterations);
        System.out.println("threads : " + numThreads);

        System.out.println("Bootstrapping cluster data.");
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(numThreads)
                       .setMaxTotalConnections(numThreads)
                       .setMaxConnectionsPerNode(numThreads)
                       .setBootstrapUrls(url)
                       .setConnectionTimeout(60, TimeUnit.SECONDS)
                       .setSocketTimeout(60, TimeUnit.SECONDS)
                       .setSocketBufferSize(4 * 1024);
        SocketStoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        final StoreClient<Integer, List<Long>> indexStore = factory.getStoreClient(indexStoreName);
        final StoreClient<Long, String> messageStore = factory.getStoreClient(messageStoreName);

        final String value = TestUtils.randomLetters(valueSize);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        final Random random = new Random();

        for (int iteration = 0; iteration < numIterations; iteration++) {
            System.out.println("======================= iteration = " + iteration
                               + " ======================================");
            if (ops.contains("w")) {
                final AtomicInteger numWrites = new AtomicInteger(0);
                System.out.println("Beginning write test.");
                final KeyProvider<Integer> indexKeyProvider = new IntegerKeyProvider(0, primaryKeys);
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                for (int i = 0; i < numRequests; i++) {
                    final int j = i;
                    final Integer primaryKey = indexKeyProvider.next();

                    executor.execute(new Runnable() {
                        public void run() {
                            int numMessages = 25 + random.nextInt(25);
                            List<Long> messages = Lists.newArrayListWithCapacity(numMessages);
                            for (int i = 0; i < numMessages; i++)
                                messages.add(random.nextLong() % MAX_MSG_GUID);

                            try {
                                long startNs = System.nanoTime();
                                indexStore.put(primaryKey, messages);

                                for (Long messageId: messages)
                                    messageStore.put(messageId, value);
                                long requestTime =  (System.nanoTime() - startNs) / Time.NS_PER_MS;
                                requestTimes[j] = requestTime;
                                numWrites.incrementAndGet();

                            } catch (Exception e) {
                                if (verbose)
                                    e.printStackTrace();
                            } finally {
                                latch.countDown();
                                if (interval != 0 && j % interval == 0) {
                                    printStatistics("writes", j, start, requestTimes);
                                }
                            }
                        }
                    });
                }
                latch.await();
            }

            if (ops.contains("r")) {
                final AtomicInteger numReads = new AtomicInteger(0);
                final AtomicInteger numNulls = new AtomicInteger(0);
                System.out.println("Beginning read test");
                final KeyProvider<Integer> indexKeyProvider = new IntegerKeyProvider(0, primaryKeys);
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                for (int i = 0; i < numRequests; i++) {
                    final int j = i;
                    final Integer primaryKey = indexKeyProvider.next();
                    executor.execute(new Runnable() {
                        public void run() {
                            try {
                                long startNs = System.nanoTime();
                                Versioned<List<Long>> v = indexStore.get(primaryKey);
                                if (v != null) {
                                    List<Long> messageIds = v.getValue();
                                    Map<Long, Versioned<String>> results = messageStore.getAll(messageIds);
                                } else {
                                    numNulls.incrementAndGet();
                                }
                                long requestTime =  (System.nanoTime() - startNs) / Time.NS_PER_MS;
                                synchronized(requestTimes) {
                                    requestTimes[j] = requestTime;
                                }
                                numReads.incrementAndGet();
                            } catch (Exception e) {
                                if (verbose)
                                    e.printStackTrace();
                            } finally {
                                latch.countDown();
                                if (interval != 0 && j % interval == 0) {
                                    printStatistics("reads", j, start, requestTimes);
                                    printNulls(numNulls.get(), start);
                                }
                            }
                        }
                    });
                }
                latch.await();
            }

            if (ops.contains("m")) {
                final AtomicInteger numAppends = new AtomicInteger(0);
                System.out.println("Beginning mixed/append test.");
                final KeyProvider<Integer> indexKeyProvider = new IntegerKeyProvider(0, primaryKeys);
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                for (int i = 0; i < numRequests; i++) {
                    final int j = i;
                    executor.execute(new Runnable() {
                        final int key = indexKeyProvider.next();
                        public void run() {
                            try {
                                long startNs = System.nanoTime();
                                insertMessage(indexStore, messageStore, key, random.nextLong() % MAX_MSG_GUID, value);
                                requestTimes[j] = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                                numAppends.incrementAndGet();
                            } catch (Exception e) {
                                if (verbose)
                                    e.printStackTrace();
                            } finally {
                                latch.countDown();
                                if (interval != 0 && j % interval == 0) {
                                    printStatistics("appends", j, start, requestTimes);
                                }
                            }
                        }
                    });
                }
                latch.await();
            }
        }
        executor.shutdown();
    }

}
