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
package org.apache.sdap.mudrod.tools;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Entry point providing ingestion logic of <a href="https://eonet.sci.gsfc.nasa.gov/">
 * Earth Observatory Natural Event Tracker (EONET)</a> data into
 * the SDAP search server.
 */
public class EONETIngester extends MudrodAbstract {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(EONETIngester.class);

  private static final String[] EVENTS_URLS = {
          "https://eonet.sci.gsfc.nasa.gov/api/v2.1/events?status=closed",
  "https://eonet.sci.gsfc.nasa.gov/api/v2.1/events?status=open"};

  public EONETIngester(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    initMudrod();
  }

  /**
   * @param args there are no arguments for this service...
   * It ingests the entire EONET database. In the future this will 
   * most likely change to provide finer grained controls over 
   * what kind of EONET data we wish to ingest e.g. recent events from the
   * last day only, etc.
   */
  public static void main(String[] args) {
    MudrodEngine mudrodEngine = checkConfigInit(new MudrodEngine());
    EONETIngester eonetIngester = new EONETIngester(
            mudrodEngine.getConfig(), mudrodEngine.getESDriver(), null);
    eonetIngester.acquireAllEvents(mudrodEngine);
  }

  private static MudrodEngine checkConfigInit(MudrodEngine mudrodEngine) {
    if (mudrodEngine.getConfig().isEmpty()) {
      mudrodEngine.loadConfig();
      mudrodEngine.setESDriver(mudrodEngine.startESDriver());
    }
    return mudrodEngine;

  }

  public String acquireAllEvents(MudrodEngine mEngine) {
    ESDriver esDriver = mEngine.getESDriver();
    if (mEngine.getConfig().isEmpty()) {
      mEngine.loadConfig();
      esDriver = mEngine.startESDriver();
    }
    String result = null;
    try {
      result = executeBulkIndexRequest(mEngine, esDriver, executeEonetGetOperations());
    } catch (SSLHandshakeException e) {
      LOG.error("SSL handshake is failed whilst acquiring all events!", e);
    }
    if (result != null) {
      return result;
    } else {
      return "";
    }
  }

  private String executeBulkIndexRequest(MudrodEngine mEngine, ESDriver esDriver, JsonArray jsonEventsArray) {
    esDriver.createBulkProcessor();
    BulkProcessor bp = esDriver.getBulkProcessor();
    GetResult result = null;
    String index = mEngine.getConfig().getProperty(MudrodConstants.ES_INDEX_NAME);
    String eventType = "eonet_event";
    //for each event
    for (JsonElement jsonElement : jsonEventsArray) {
      UpdateRequest updateRequest = null;
      JsonObject event = jsonElement.getAsJsonObject();
      String eventId = event.get("id").toString();
      try {
        IndexRequest indexRequest = new IndexRequest(
                index, eventType, eventId).source(executeEventMapping(event));
        updateRequest =
                new UpdateRequest(index, eventType, eventId).upsert(indexRequest);
        updateRequest.doc(indexRequest); 
        bp.add(updateRequest);
      } catch (NumberFormatException e) {
        LOG.error("Error whilst processing numbers", e);
      }
      UpdateResponse updateResponse = null;
      try {
        updateResponse = esDriver.getClient().update(updateRequest).get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to execute bulk Index request : ", e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
      if (updateResponse != null) {
        result = updateResponse.getGetResult();
      }
    }
    esDriver.destroyBulkProcessor();
    //return result.getSource().toString();
    return "";
  }

  private XContentBuilder executeEventMapping(JsonObject event) {
    //for an individual event
    XContentBuilder eventMapping = null;
    if (null != event.get("closed")) {
      try {
        eventMapping = jsonBuilder()
                .startObject()
                .field("id", event.get("id"))
                .field("title", event.get("title"))
                .field("description", event.get("description"))
                .field("link", event.get("link"))
                .field("closed", event.get("closed"))
                .field("categories", event.get("categories").getAsJsonArray())
                .field("sources", event.get("sources").getAsJsonArray())
                .field("geometries", event.get("geometries").getAsJsonArray())
                .endObject();
      } catch (IOException e) {
        LOG.error("Failed to create event mapping : ", e);
      }
    } else {
      try {
        eventMapping = jsonBuilder()
                .startObject()
                .field("id", event.get("id"))
                .field("title", event.get("title"))
                .field("description", event.get("description"))
                .field("link", event.get("link"))
                .field("categories", event.get("categories").getAsJsonArray())
                .field("sources", event.get("sources").getAsJsonArray())
                .field("geometries", event.get("geometries").getAsJsonArray())
                .endObject();
      } catch (IOException e) {
        LOG.error("Failed to create event mapping : ", e);
      }
    }
    return eventMapping;
  }

  private JsonArray executeEonetGetOperations() throws SSLHandshakeException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpResponse response = null;
    JsonArray eventArray = new JsonArray();
    for (String string : EVENTS_URLS) {
      HttpGet request = new HttpGet(string);
      // add request header
      request.addHeader("User-Agent", "Apache SDAP MUDROD EONETIngester");
      LOG.info("Executing: {}", request.toString());
      try {
        response = client.execute(request);
        HttpEntity entity = response.getEntity();
        JsonArray partialEventsArray = extractEventsArrayFromJsonResponse(EntityUtils.toString(entity, "UTF-8"));
        for (JsonElement jsonElement : partialEventsArray) {
          eventArray.add(jsonElement);
        }
      } catch (IOException e) {
        LOG.error("Failed to execute HTTP GET " + request.toString() + " : ", e);
      }
    }
    return eventArray;
  }

  private JsonArray extractEventsArrayFromJsonResponse(String string) {
    return ((JsonObject) new JsonParser().parse(string)).getAsJsonArray("events");
  }

}
