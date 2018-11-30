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
package org.apache.sdap.mudrod.main;

import org.apache.commons.io.IOUtils;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.EmbeddedElasticsearchServer;
import org.elasticsearch.client.Client;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

public class ESDriverTest extends AbstractElasticsearchIntegrationTest{

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  protected static final String ES_SETTINGS = "elastic_settings.json";
  protected static final String ES_MAPPINGS = "elastic_mappings.json";
  
  static ESDriver es = null;
  static MudrodEngine mudrodEngine = null;
  @BeforeClass
  public static void setUp(){
	 mudrodEngine = new MudrodEngine();
	 es = new ESDriver(mudrodEngine.loadConfig());	 
  }

  @AfterClass
  public static void tearDown(){
     //es.destroyBulkProcessor();
     //es.close();
  }
  
  @Test
  public void testESDriverProperties() {
   
    Client client = es.getClient();
    assert client != null;
  }

  @Test
  public void testCreateBulkProcessor() {
    es.createBulkProcessor();
    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testDestroyBulkProcessor() {
    es.createBulkProcessor();
    es.destroyBulkProcessor();

    BulkProcessor processor = es.getBulkProcessor();
    assert processor == null;
  }

  @Test
  public void testPutMapping() {  

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
    es.deleteAllByQuery("mudrod", "MetadataLinkage", QueryBuilders.matchAllQuery());

    // String res = es.searchByQuery("mudrod", "MetadataLinkage",
    // QueryBuilders.matchAllQuery());
    // assert res == ""||res == null;
  }

  @Test
  public void testDeleteType() {
        
    es.deleteType("mudrod", "MetadataLinkage");
  }

  @Test
  public void testGetTypeListWithPrefix() {
    
    es.getTypeListWithPrefix("podaacsession", "sessionstats");
  }

  @Test
  public void testGetIndexListWithPrefix() {    

    List<String> res = es.getIndexListWithPrefix("podaacsession");
    assert !res.isEmpty();
  }

  @Test
  public void testSearchByQueryStringStringString() {

    try {
      String res = es.searchByQuery("mudrod", "MetadataLinkage", "temp");
      assert res != null;
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testSearchByQueryStringStringStringBoolean() {

    try {
      String res = es.searchByQuery("mudrod", "MetadataLinkage", "temp", false);
      assert res != null;
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Error!", e);
    }
  }

  @Test
  public void testAutoComplete() {

    List<String> res = es.autoComplete("mudrod", "term");
    assert !res.isEmpty();
  }

  @Test
  public void testClose() {

    es.close();
  }

  @Test
  public void testRefreshIndex() {
    
    es.refreshIndex();
  }

/*  @Test
  public void testMakeClient() {

    try {
      Client client = es.makeClient(mudrodEngine.loadConfig());
      assert client != null;
    } catch (IOException e) {
      LOG.error("Error!", e);
    }
  }*/

  @Test
  public void testGetClient() {

    Client client = es.getClient();
    assert client != null;
  }

/*  @Test
  public void testSetClient() {
    
    try {
      Client client = es.makeClient(mudrodEngine.loadConfig());
      es.setClient(client);
      Client res = es.getClient();
      assert res != null;
    } catch (IOException e) {
      LOG.error("Error!", e);
    }
  }
*/
  @Test
  public void testGetBulkProcessor() {
        
    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testSetBulkProcessor() {
      
    BulkProcessor begin = es.getBulkProcessor();
    es.setBulkProcessor(begin);

    BulkProcessor processor = es.getBulkProcessor();
    assert processor != null;
  }

  @Test
  public void testGenerateUpdateRequestStringStringStringStringObject() {
    
    UpdateRequest res = es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", "temp", "string_a");
    assert res != null;
  }

  @Test
  public void testGenerateUpdateRequestStringStringStringMapOfStringObject() {
    
    Map<String, Object> result = new HashMap<String, Object>();
    result.put("temp", "string_a");

    UpdateRequest res = es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", result);
    assert res != null;
  }

  @Test
  public void testGetDocCountStringStringArray() {
    
    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount("mudrod", box);

    assert res > 0;
  }

  @Test
  public void testGetDocCountStringArrayStringArray() {
    
    String begin[] = new String[] { "mudrod" };
    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount(begin, box);
    assert res > 0;
  }

  @Test
  public void testGetDocCountStringArrayStringArrayQueryBuilder() {
    
    MatchAllQueryBuilder search = QueryBuilders.matchAllQuery();

    String begin[] = new String[] { "mudrod" };
    String box[] = new String[] { "MetadataLinkage" };
    int res = es.getDocCount(begin, box, search);
    assert res > 0;
  }
}
