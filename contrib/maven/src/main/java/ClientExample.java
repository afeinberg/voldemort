import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

public class ClientExample {

    public static void main(String[] args) {

        // In real life this stuff would get wired in
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        StoreClient<String, String> client = factory.getStoreClient("test");


        // update the value
        client.put("some_key", "hi sir");
        
        // get the value
        Versioned<String> version = client.get("some_key");

        // modify the value
        version.setObject("new_value");

    }

}