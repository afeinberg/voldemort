package voldemort.performance;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * @author afeinber
 */
public class RequestFileGenerator {
    private final StoreDefinition storeDefinition;
    private final RoutingStrategy routingStrategy;
    private final String inputFile;
    private final String outputFile;
    private final Node node;

    public RequestFileGenerator(StoreDefinition storeDefinition,
                            RoutingStrategy routingStrategy,
                            String inputFile,
                            String outputFile,
                            Node node) {
        this.storeDefinition = storeDefinition;
        this.routingStrategy = routingStrategy;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.node = node;
    }

    public void generate() throws IOException {
        SerializerFactory factory = new DefaultSerializerFactory();
        @SuppressWarnings("unchecked")
        Serializer<Integer> keySerializer = (Serializer<Integer>) factory.getSerializer(storeDefinition.getKeySerializer());

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
        try {
            String line = null;
            while ((line = in.readLine()) != null) {
                int key = Integer.valueOf(line.replaceAll("\\s+", ""));
                byte[] keyBytes = keySerializer.toBytes(key);
                List<Node> nodes = routingStrategy.routeRequest(keyBytes);
                if (nodes.contains(node)) {
                    out.write(key + "\n");
                }
            }
        } finally {
            in.close();
            out.close();
        }
    }

    public static void main(String [] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("node", "[REQUIRED] node id")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("node id");
        parser.accepts("store-name", "[REQUIRED] store name")
                       .withRequiredArg()
                       .describedAs("store name");
        parser.accepts("cluster", "[REQUIRED] path to cluster xml config file")
                       .withRequiredArg()
                       .describedAs("cluster.xml");
        parser.accepts("stores", "[REQUIRED] path to stores xml config file")
                       .withRequiredArg()
                       .describedAs("stores.xml");
        parser.accepts("input", "[REQUIRED] input file to read from")
                       .withRequiredArg()
                       .describedAs("input-file");
        parser.accepts("output", "[REQUIRED] output file to write to")
                       .withRequiredArg()
                       .describedAs("output-file");
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "node",
                                               "cluster",
                                               "stores",
                                               "store-name",
                                               "input",
                                               "output");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        int nodeId = (Integer) options.valueOf("node");
        String clusterFile = (String) options.valueOf("cluster");
        String storeDefFile = (String) options.valueOf("stores");
        String storeName = (String) options.valueOf("store-name");
        String inputFile = (String) options.valueOf("input");
        String outputFile = (String) options.valueOf("output");

        try {
            Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));
            StoreDefinition storeDefinition = null;
            List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeDefFile)));
            for (StoreDefinition def: stores) {
                if (def.getName().equals(storeName))
                    storeDefinition = def;
            }
            if (storeDefinition == null)
                Utils.croak("No store found with name \"" + storeName + "\"");

            Node node = null;
            try {
                node = cluster.getNodeById(nodeId);
            } catch (VoldemortException e) {
                Utils.croak("Can't find a node with id " + nodeId);
            }

            RoutingStrategy routingStrategy = new RoutingStrategyFactory(cluster).getRoutingStrategy(storeDefinition);

            new RequestFileGenerator(storeDefinition,
                                 routingStrategy,
                                 inputFile,
                                 outputFile,
                                 node).generate();
        } catch (FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
    }

    
}
