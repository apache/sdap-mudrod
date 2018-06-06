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
package org.apache.sdap.mudrod.storage;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.sdap.mudrod.storage.elasticsearch.ElasticSearchDriver;
import org.apache.sdap.mudrod.storage.solr.SolrDriver;

/**
 * Core storage datum from which all concrete storage-related 
 * implementations should extend.
 * @since v0.0.1-SNAPSHOT
 */
public interface StorageDriver {

  /**
   * 
   * @return an initialized {@link org.apache.sdap.mudrod.storage.StorageDriver} implementation.
   */
  default StorageDriver initialize(Properties props) {
    StorageDriver sDriver = null;
    if (props != null) {
      switch (props.getProperty("mudrod.storage.driver", "elasticsearch")) {
        case "elasticsearch":
        sDriver = new ElasticSearchDriver(props);
        break;
        default:
        sDriver = new SolrDriver(props);
        break;
      }
    } else {

    }
    return sDriver;
  }

  abstract void createBulkProcessor();

  abstract void destroyBulkProcessor();

  abstract void putMapping(String indexName, String settingsJson, String mappingJson) throws IOException;

  abstract String customAnalyzing(String indexName, String str) throws InterruptedException, ExecutionException;

  abstract String customAnalyzing(String indexName, String analyzer, String str) throws InterruptedException, ExecutionException;

  abstract List<String> customAnalyzing(String indexName, List<String> list) throws InterruptedException, ExecutionException;

  //abstract void deleteAllByQuery(String index, String type, QueryBuilder query);

  abstract void deleteType(String index, String type);

  abstract List<String> getTypeListWithPrefix(Object object, Object object2);

  abstract  List<String> getIndexListWithPrefix(Object object);

  abstract String searchByQuery(String index, String type, String query);

  abstract String searchByQuery(String index, String type, String query, Boolean bDetail);

  abstract List<List<String>> buildMeasurementHierarchies(
          List<String> topics, List<String> terms, List<String> variables, 
          List<String> variableDetails);

  abstract List<String> autoComplete(String index, String term);

  abstract void close();

  abstract void refreshIndex();

  //abstract Client makeClient(Properties props);

  //abstract UpdateRequest generateUpdateRequest(String index, 
  // String type, String id, String field1, Object value1);

  //UpdateRequest generateUpdateRequest(String index, String type, String id,
  // Map<String, Object> filedValueMap);

  abstract int getDocCount(String index, String... type);

  abstract int getDocCount(String[] index, String[] type);

  //public int getDocCount(String[] index, String[] type, QueryBuilder filterSearch);

}
