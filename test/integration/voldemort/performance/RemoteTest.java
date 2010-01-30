/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.performance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.store.Store;
import voldemort.utils.CmdUtils;
import voldemort.versioning.Occured;
import voldemort.versioning.Versioned;

public class RemoteTest {

    public static final int MAX_WORKERS = 8;

    public static class KeyProvider {

        private final List<Integer> keys;
        private final AtomicInteger index;

        public KeyProvider(int start, List<Integer> keys) {
            this.index = new AtomicInteger(start);
            this.keys = keys;
        }

        public String nextString() {
            return Integer.toString(nextInt());
        }

        public int nextInt() {
            if (keys != null) {
                return keys.get(index.getAndIncrement() % keys.size());
            } else {
                return index.getAndIncrement();
            }
        }
    }

    public static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/bin/remote-test.sh \\");
        out.println("          [options] bootstrapUrl storeName num-requests\n");
        parser.printHelpOn(out);
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("r", "execute read operations");
        parser.accepts("m", "execute a mix of reads and writes");
        parser.accepts("w", "execute write operations");
        parser.accepts("d", "execute delete operations");
        parser.accepts("v", "verbose mode");
        parser.accepts("no-handshake", "skip \"handshake\"");
        parser.accepts("verbose", "verbose mode on: print stack traces");
        parser.accepts("integer-keys", "use integer keys");
        parser.accepts("request-file", "execute specific requests in order").withRequiredArg();
        parser.accepts("start-key-index", "starting point when using int keys. Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("value-size", "size in bytes for random value.  Default = 1024")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("iterations", "number of times to repeat the test  Default = 1")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("threads", "max number concurrent worker threads  Default = " + MAX_WORKERS)
              .withRequiredArg()
              .ofType(Integer.class);
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            printUsage(System.out, parser);
            System.exit(0);
        }
        
        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() != 3) {
            printUsage(System.err, parser);
            System.exit(1);
        }

        String url = nonOptions.get(0);
        String storeName = nonOptions.get(1);
        int numRequests = Integer.parseInt(nonOptions.get(2));
        String ops = "";
        List<Integer> keys = null;

        Integer startNum = CmdUtils.valueOf(options, "start-key-index", 0);
        Integer valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        Integer numIterations = CmdUtils.valueOf(options, "iterations", 1);
        Integer numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        final boolean intKeys = options.has("integer-keys");
        final boolean verbose = options.has("verbose");
        boolean handshake = !options.has("no-handshake");

        if(options.has("request-file")) {
            keys = loadKeys((String) options.valueOf("request-file"));
        }

        if(options.has("r")) {
            ops += "r";
        }
        if(options.has("w")) {
            ops += "w";
        }
        if(options.has("d")) {
            ops += "d";
        }
        if (options.has("m")) {
            ops += "m";
        }
        if(ops.length() == 0) {
            ops = "rwd";
        }

        System.out.println("operations : " + ops);
        System.out.println("value size : " + valueSize);
        System.out.println("start index : " + startNum);
        System.out.println("iterations : " + numIterations);
        System.out.println("threads : " + numThreads);

        System.out.println("Bootstraping cluster data.");
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setMaxThreads(numThreads)
                                                                                    .setMaxTotalConnections(numThreads)
                                                                                    .setMaxConnectionsPerNode(numThreads)
                                                                                    .setBootstrapUrls(url)
                                                                                    .setConnectionTimeout(60,
                                                                                                          TimeUnit.SECONDS)
                                                                                    .setSocketTimeout(60,
                                                                                                      TimeUnit.SECONDS)
                                                                                    .setSocketBufferSize(4 * 1024));
        final StoreClient<Object, Object> store = factory.getStoreClient(storeName);
        final String value = TestUtils.randomLetters(valueSize);
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        /*
         * send the store a value and then delete it - useful for the NOOP store
         * which will then use that value for other queries
         */

        if (handshake) {
            KeyProvider keyProvider = new KeyProvider(startNum, keys);
            final Object key = intKeys ? keyProvider.nextInt() : keyProvider.nextString();

            // We need to delete just in case there's an existing value there that
            // would otherwise cause the test run to bomb out.
            store.delete(key);
            store.put(key, new Versioned<String>(value));
            store.delete(key);
        }

        for(int loopCount = 0; loopCount < numIterations; loopCount++) {

            System.out.println("======================= iteration = " + loopCount
                               + " ======================================");

            if(ops.contains("d")) {
                System.out.println("Beginning delete test.");
                final AtomicInteger successes = new AtomicInteger(0);
                final KeyProvider keyProvider0 = new KeyProvider(startNum, keys);
                final CountDownLatch latch0 = new CountDownLatch(numRequests);
                final AtomicInteger exceptions = new AtomicInteger(0);
                long start = System.currentTimeMillis();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                store.delete(keyProvider0.nextString());
                                successes.getAndIncrement();
                            } catch(Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                                exceptions.incrementAndGet();
                            } finally {
                                latch0.countDown();
                            }
                        }
                    });
                }
                latch0.await();
                long deleteTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) deleteTime * 1000)
                                   + " deletes/sec.");
                System.out.println(successes.get() + " things deleted.");
            }

            if(ops.contains("w")) {
                System.out.println("Beginning write test.");
                final KeyProvider keyProvider1 = new KeyProvider(startNum, keys);
                final CountDownLatch latch1 = new CountDownLatch(numRequests);
                final AtomicInteger exceptions = new AtomicInteger(0);
                long start = System.currentTimeMillis();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Object key = intKeys ? keyProvider1.nextInt() : keyProvider1.nextString();
                                store.put(key, value);
                            } catch(Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                                exceptions.incrementAndGet();
                            } finally {
                                latch1.countDown();
                            }
                        }
                    });
                }
                latch1.await();
                long writeTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) writeTime * 1000)
                                   + " writes/sec.");
            }

            if (ops.contains("m")) {
                System.out.println("Beginning mixed read/write test.");
                final KeyProvider keyProvider3 = new KeyProvider(startNum, keys);
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final AtomicInteger reads = new AtomicInteger(0);
                final AtomicInteger writes = new AtomicInteger(0);
                final AtomicInteger exceptions = new AtomicInteger(0);
                long start = System.currentTimeMillis();
                for (int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Object key = intKeys ? keyProvider3.nextInt() : keyProvider3.nextString();
                                Versioned<Object> v = store.get(key);
                                reads.incrementAndGet();

                                if (v == null) {
                                    throw new Exception("value return is null for key " + key);
                                }
                                // write my read
                                store.put(key, v.getValue());
                                writes.incrementAndGet();
                                Versioned<Object> v2 = store.get(key);
                                reads.incrementAndGet();
                                Occured occured = v2.getVersion().compare(v.getVersion());
                                if (occured != Occured.AFTER) {
                                    System.err.println("clock for v1: " + v.getVersion() + "\n"
                                                       + "clock for v2: " + v2.getVersion());
                                    throw new Exception("stale value for key " + key);
                                }
                            } catch (Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                                exceptions.incrementAndGet();
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }
                latch.await();
                long time = System.currentTimeMillis() - start;
                int writesCnt = writes.get();
                int opsCnt = reads.get() + writesCnt;
                int excptCnt = exceptions.get();
                System.out.println("Operation throughput: " + (opsCnt / (float) time * 1000) + " ops/sec.");
                System.out.println("Transaction throughout: " + (numRequests / (float) time * 1000) + " txns/sec.");
                System.out.println("Workload: " + numRequests + " transactions, "
                                   + opsCnt + " operations. "
                                   + Math.round(writesCnt / (float) (reads.get() + writesCnt) * 100)
                                   + "% writes.");
                System.out.println("Exceptions: " + excptCnt + " total. "
                                   + Math.round(excptCnt / (float) opsCnt * 100)
                                   +"% of operations, "
                                   + Math.round(excptCnt / (float) numRequests * 100)
                                   +"% of transactions.");
            }

            if(ops.contains("r")) {
                System.out.println("Beginning read test.");
                final KeyProvider keyProvider2 = new KeyProvider(startNum, keys);
                final CountDownLatch latch2 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                keyProvider2.nextString();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Object key = intKeys ? keyProvider2.nextInt() : keyProvider2.nextString();
                                Versioned<Object> v = store.get(key);

                                if(v == null) {
                                    throw new Exception("value returned is null for key " + key);
                                }

                                if(value.equals(v.getValue())) {
                                    throw new Exception("value returned isn't same as set value.");
                                }

                            } catch(Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                            } finally {
                                latch2.countDown();
                            }
                        }
                    });
                }
                latch2.await();
                long readTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) readTime * 1000.0)
                                   + " reads/sec.");
            }
        }

        System.exit(0);
    }

    public static List<Integer> loadKeys(String path) throws IOException {

        List<Integer> targets = new ArrayList<Integer>();
        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null) {
                targets.add(Integer.valueOf(text.replaceAll("\\s+", "")));
            }
        } finally {
            try {
                if(reader != null) {
                    reader.close();
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        return targets;
    }
}
