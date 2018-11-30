package org.apache.sdap.mudrod.main;

import org.apache.sdap.mudrod.driver.EmbeddedElasticsearchServer;
import org.apache.sdap.mudrod.driver.EmbeddedElasticsearchServer;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This is a helper class the starts an embedded elasticsearch server
 * for each test.
 *
 * @author Felix Müller
 */
public abstract class AbstractElasticsearchIntegrationTest {

    private static EmbeddedElasticsearchServer embeddedElasticsearchServer;

    @BeforeClass
    public static void startEmbeddedElasticsearchServer() {
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
    }

    @AfterClass
    public static void shutdownEmbeddedElasticsearchServer() {
        //embeddedElasticsearchServer.shutdown();
    }

    /**
     * By using this method you can access the embedded server.
     */
    protected Client getClient() {
        return embeddedElasticsearchServer.getClient();
    }
}
