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
import org.apache.sdap.mudrod.main.MudrodEngine;
import java.io.File;
import java.io.IOException;


public class EmbeddedElasticsearchServer {

  private static final String DEFAULT_DATA_DIRECTORY = "/Users/kevin/elasticsearch-5.2.2/data";
  private static final String DEFAULT_PATH_DIRECTORY = "/Users/kevin/elasticsearch-5.2.2/home/dir";

  private final Node node;
  private final String dataDirectory;

  public EmbeddedElasticsearchServer() {
      this(DEFAULT_DATA_DIRECTORY);
  }

  public EmbeddedElasticsearchServer(String dataDirectory) {
      this.dataDirectory = dataDirectory;
      MudrodEngine mudrodEngine = new MudrodEngine();
      
      Settings.Builder elasticsearchSettings = Settings.builder()
              .put("http.enabled", "false")
              .put("path.data", dataDirectory)
              .put("path.home", DEFAULT_PATH_DIRECTORY)
              .put(mudrodEngine.loadConfig());

      node = new Node(elasticsearchSettings.build());
  }

  public Client getClient() {
      return node.client();
  }

  public void shutdown() {
    try {
      node.close();
    } catch (IOException e) {
      System.out.println(e);
    }
      deleteDataDirectory();
  }

  private void deleteDataDirectory() {
      try {
          FileUtils.deleteDirectory(new File(dataDirectory));
      } catch (IOException e) {
          throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
      }
  }
}