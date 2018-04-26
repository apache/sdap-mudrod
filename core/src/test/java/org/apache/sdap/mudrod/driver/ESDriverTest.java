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

import org.apache.commons.io.IOUtils;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.driver.EmbeddedElasticsearchServer;
import org.elasticsearch.client.Client;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ESDriverTest {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  protected static final String ES_SETTINGS = "elastic_settings.json";
  protected static final String ES_MAPPINGS = "elastic_mappings.json";

  @Test
  public void testESDriver() {
    ESDriver es = new ESDriver();
  }

  @Test
  public void testESDriverProperties() {
    EmbeddedElasticsearchServer tempes = new EmbeddedElasticsearchServer();
    Client client = tempes.getClient();
    assert client != null;
  }

  @Test
  public void testCreateBulkProcessor() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.createBulkProcessor();

    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testDestroyBulkProcessor() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.createBulkProcessor();
    es.destroyBulkProcessor();

    BulkProcessor processor = es.getBulkProcessor();
    assert processor == null;
  }

  @Test
  public void testPutMapping() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    InputStream settingsStream = getClass().getClassLoader().getResourceAsStream(ES_SETTINGS);
    InputStream mappingsStream = getClass().getClassLoader().getResourceAsStream(ES_MAPPINGS);

    JSONObject settingsJSON = null;
    JSONObject mappingJSON = null;

    Properties props = mudrodEngine.loadConfig();
    try {
      settingsJSON = new JSONObject(IOUtils.toString(settingsStream));
    } catch (JSONException | IOException e1) {
      LOG.error("Error reading Elasticsearch settings!", e1);
    }

    try {
      mappingJSON = new JSONObject(IOUtils.toString(mappingsStream));
    } catch (JSONException | IOException e1) {
      LOG.error("Error reading Elasticsearch mappings!", e1);
    }

    try {
      if (settingsJSON != null && mappingJSON != null) {
        es.putMapping(props.getProperty(MudrodConstants.ES_INDEX_NAME), settingsJSON.toString(), mappingJSON.toString());
      }
    } catch (IOException e) {
      LOG.error("Error entering Elasticsearch Mappings!", e);
    }
  }

  @Test
  public void testCustomAnalyzingStringString() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    String str = "temp";

    try {
      String res = es.customAnalyzing("mudrod", str);
      assert res != "";
      assert res != null;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testCustomAnalyzingStringStringString() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    String str = "temp";

    try {
      String res = es.customAnalyzing("mudrod", "cody", str);
      assert res != "";
      assert res != null;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testCustomAnalyzingStringListOfString() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    List<String> customlist = new ArrayList<>();
    customlist.add("string_a");
    customlist.add("string_b");

    try {
      List<String> res = es.customAnalyzing("mudrod", customlist);
      assert !res.isEmpty();
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testDeleteAllByQuery() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.deleteAllByQuery("mudrod", "MetadataLinkage", QueryBuilders.matchAllQuery());

    // String res = es.searchByQuery("mudrod", "MetadataLinkage",
    // QueryBuilders.matchAllQuery());
    // assert res == ""||res == null;
  }

  @Test
  public void testDeleteType() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.deleteType("mudrod", "MetadataLinkage");
  }

  @Test
  public void testGetTypeListWithPrefix() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.getTypeListWithPrefix("podaacsession", "sessionstats");
  }

  @Test
  public void testGetIndexListWithPrefix() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    List<String> res = es.getIndexListWithPrefix("podaacsession");
    assert !res.isEmpty();
  }

  @Test
  public void testSearchByQueryStringStringString() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    try {
      String res = es.searchByQuery("mudrod", "MetadataLinkage", "temp");
      assert res != null;
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testSearchByQueryStringStringStringBoolean() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    try {
      String res = es.searchByQuery("mudrod", "MetadataLinkage", "temp", false);
      assert res != null;
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testAutoComplete() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    List<String> res = es.autoComplete("mudrod", "term");
    assert !res.isEmpty();
  }

  @Test
  public void testClose() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    es.close();
  }

  @Test
  public void testRefreshIndex() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    es.refreshIndex();
  }

  @Test
  public void testMakeClient() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    try {
      Client client = es.makeClient(mudrodEngine.loadConfig());
      assert client != null;
    } catch (IOException e) {
      LOG.error("Error!", e);
    }
  }

  /*
   * @Test public void testMain() { fail("Not yet implemented"); }
   */
  @Test
  public void testGetClient() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    Client client = es.getClient();
    assert client != null;
  }

  @Test
  public void testSetClient() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    try {
      Client client = es.makeClient(mudrodEngine.loadConfig());
      es.setClient(client);
      Client res = es.getClient();
      assert res != null;
    } catch (IOException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testGetBulkProcessor() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testSetBulkProcessor() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    BulkProcessor begin = es.getBulkProcessor();
    es.setBulkProcessor(begin);

    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testGenerateUpdateRequestStringStringStringStringObject() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    UpdateRequest res = es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", "temp", "string_a");
    assert res != null;
  }

  @Test
  public void testGenerateUpdateRequestStringStringStringMapOfStringObject() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    Map<String, Object> result = new HashMap<String, Object>();
    result.put("temp", "string_a");

    UpdateRequest res = es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", result);
    assert res != null;
  }

  @Test
  public void testGetDocCountStringStringArray() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount("mudrod", box);

    assert res > 0;
  }

  @Test
  public void testGetDocCountStringArrayStringArray() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    String begin[] = new String[] { "mudrod" };
    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount(begin, box);
    assert res > 0;
  }

  @Test
  public void testGetDocCountStringArrayStringArrayQueryBuilder() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());

    MatchAllQueryBuilder search = QueryBuilders.matchAllQuery();

    String begin[] = new String[] { "mudrod" };
    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount(begin, box, search);
    assert res > 0;
  }
}
