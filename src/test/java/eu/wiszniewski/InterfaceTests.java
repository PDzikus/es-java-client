package eu.wiszniewski;

import eu.wiszniewski.client.ElasticSearchClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Test ElasticSearchClient")
public class InterfaceTests {

    @Test
    @DisplayName("Testing creation of ElasticSearchClient objects")
    void clientCreationTest() {
        String address = "10.10.10.10";
        int port = 9200;

        ElasticSearchClient clientWithDefaultPortNumber = new ElasticSearchClient(address);
        ElasticSearchClient client = new ElasticSearchClient(address, port);
        assertNotNull(client);
        assertNotNull(clientWithDefaultPortNumber);
        assertThrows(IllegalArgumentException.class, () ->  new ElasticSearchClient("", 9000) );
        assertDoesNotThrow(client::close);
    }

}
