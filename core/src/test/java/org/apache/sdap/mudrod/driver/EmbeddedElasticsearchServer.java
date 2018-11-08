package org.apache.sdap.mudrod.driver;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.Netty3Plugin;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Example implementation of an embedded elasticsearch server.
 *
 * @author Felix Müller
 */
public class EmbeddedElasticsearchServer {

  private static final String DEFAULT_DATA_DIRECTORY = "target/elasticsearch-data";

  private Node node;
  private final String dataDirectory;

  public EmbeddedElasticsearchServer() {
    this(DEFAULT_DATA_DIRECTORY);
  }

  public EmbeddedElasticsearchServer(String dataDirectory) {
    this.dataDirectory = dataDirectory;

    Settings.Builder settingsBuilder = Settings.builder();
    settingsBuilder.put("http.type", "netty3");
    settingsBuilder.put("transport.type", "netty3");
    settingsBuilder.put("network.host", "127.0.0.1");
    settingsBuilder.put("transport.tcp.port", 9300);

    settingsBuilder.put("cluster.name", "MurdorES").put("http.enabled", "false").put("path.data", dataDirectory).put("path.home", "/");
      
    Settings settings = settingsBuilder.build();
    Collection plugins = Arrays.asList(Netty3Plugin.class);
    node = null;
    try {
      node = new PluginConfigurableNode(settings, plugins).start();
      System.out.println(node.toString());
    } catch (NodeValidationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    System.out.println(node.getNodeEnvironment().nodeId());
   
  }

  public Client getClient() {
    return node.client();
  }

  public void shutdown() {
    /*try {
      node.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    deleteDataDirectory();*/
  }

  private void deleteDataDirectory() {
    try {
      FileUtils.deleteDirectory(new File(dataDirectory));
    } catch (IOException e) {
      throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
    }
  }
}
