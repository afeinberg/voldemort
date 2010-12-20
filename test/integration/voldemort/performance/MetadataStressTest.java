package voldemort.performance;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.xml.ClusterMapper;
import voldemort.xml.MappingException;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.StringReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class MetadataStressTest {

    public static void main(String[] args) throws Exception {

        if(args.length < 3) {
            System.err.println("java voldemort.performance.MetadataStressTest url iterations threads selectors");
            System.exit(-1);
        }

        String url = args[0];
        final int count = Integer.parseInt(args[1]);
        int numThreads = Integer.parseInt(args[2]);
        int numSelectors = args.length > 3 ? Integer.parseInt(args[3]) : 8;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads,
                                                                new ThreadFactory() {

                                                                    public Thread newThread(Runnable r) {
                                                                        Thread thread = new Thread(r);
                                                                        thread.setName("stress-test");
                                                                        return thread;
                                                                    }
                                                                });
        try {
            final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(url)
                                                                                                    .setMaxThreads(numThreads)
                                                                                                    .setSelectors(numSelectors));
            for(int i = 0; i < numThreads; i++) {
                executor.submit(new Runnable() {

                    public void run() {
                        for(int j = 0; j < count; j++)  {
                            try {
                                String clusterXml = factory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);
                                Cluster cluster = new ClusterMapper().readCluster(new StringReader(clusterXml));
                                String storesXml = factory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
                                List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml));
                                System.out.println("ok " + j);
                            } catch(MappingException me) {
                                me.printStackTrace();
                                System.exit(-1);
                            } catch(Exception e) {
                                // Don't fail on non XML exceptions, just continue on
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        } finally {
          executor.shutdown();
        }
    }
}
