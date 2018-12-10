/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * embedded elasticsearch server.
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
    settingsBuilder.put("cluster.name", "MurdorES").put("http.enabled", "false").put("path.data", dataDirectory).put("path.home", "/");

    Settings settings = settingsBuilder.build();
    Collection plugins = Arrays.asList(Netty3Plugin.class);
    node = null;
    try {
      node = new PluginConfigurableNode(settings, plugins).start();
      System.out.println(node.toString());
    } catch (NodeValidationException e) {
      e.printStackTrace();
    }

    System.out.println(node.getNodeEnvironment().nodeId());
  }

  public Client getClient() {
    return node.client();
  }

  public void shutdown() {
    try {
      node.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deleteDataDirectory() {
    try {
      FileUtils.deleteDirectory(new File(dataDirectory));
    } catch (IOException e) {
      throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
    }
  }
}
