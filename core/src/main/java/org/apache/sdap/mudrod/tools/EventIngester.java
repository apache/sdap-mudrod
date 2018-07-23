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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Entry point providing ingestion logic of <a href="https://eonet.sci.gsfc.nasa.gov/">
 * Earth Observatory Natural Event Tracker (EONET)</a> data into
 * the SDAP search server.
 */
public class EventIngester extends MudrodAbstract {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(EventIngester.class);

  private static final String[] EVENTS_URLS = {
          "https://eonet.sci.gsfc.nasa.gov/api/v2.1/events?status=closed",
  "https://eonet.sci.gsfc.nasa.gov/api/v2.1/events?status=open"};

  public EventIngester(Properties props, ESDriver es, SparkDriver spark) {
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
    EventIngester eonetIngester = new EventIngester(
            mudrodEngine.getConfig(), mudrodEngine.getESDriver(), null);
    eonetIngester.ingestAllEonetEvents(mudrodEngine);
    eonetIngester.correlateAndUpdateDatasetMetadataWithEONETEvents(mudrodEngine);
  }

  /**
   * @param mudrodEngine a populated, configured {@link org.apache.sdap.mudrod.main.MudrodEngine}
   * with indexing backend already configured.
   */
  private void correlateAndUpdateDatasetMetadataWithEONETEvents(MudrodEngine mudrodEngine) {
    LOG.info("Beginning event correlation and update of collection records.");
    Properties props = mudrodEngine.getConfig();
    ESDriver esDriver = mudrodEngine.getESDriver();
    String esIndex = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    String type = props.getProperty(MudrodConstants.RAW_METADATA_TYPE);
    @SuppressWarnings("unused")
    Result getResult = null;
    //1) query the eonet_event type for all records
    List<Map<String, Object>> allEonetDocs = esDriver.getAllDocs(esIndex, "eonet_event");

    //create List of CoordinateGeometry objects
    List<CoordinateGeometry> coordGeometryList = new ArrayList<>();

    //2) for each eonet_event, obtain spatial and temporal characteristics
    for (Map<String, Object> individualEventMap : allEonetDocs) {
      JsonElement eventId = new JsonParser().parse(individualEventMap.get("id").toString());
      JsonElement eventTitle = new JsonParser().parse(individualEventMap.get("title").toString());
      JsonElement eventDescription = new JsonParser().parse(individualEventMap.get("description").toString());
      JsonElement eventLink = new JsonParser().parse(individualEventMap.get("link").toString());
      JsonElement eventClosedDate = null;
      if (individualEventMap.get("closed") != null) {
        eventClosedDate = new JsonParser().parse(individualEventMap.get("closed").toString());
      }
      JsonArray categoriesArray = new JsonParser().parse(individualEventMap.get("categories").toString()).getAsJsonArray();
      JsonArray sourcesArray = new JsonParser().parse(individualEventMap.get("sources").toString()).getAsJsonArray();
      JsonArray geometriesArray = new JsonParser().parse(individualEventMap.get("geometries").toString()).getAsJsonArray();
      for (JsonElement individualGeometry : geometriesArray) {
        JsonObject individualGeometryObj = individualGeometry.getAsJsonObject();
        String geometryDate = individualGeometryObj.get("date").getAsString();
        String geometryType = individualGeometryObj.get("type").getAsString();
        if ("Point".equals(geometryType)) {
          List<String> geometryCoordinatesList = new ArrayList<>();
          JsonArray westLonNorthLat = individualGeometryObj.get("coordinates").getAsJsonArray();
          geometryCoordinatesList.add(westLonNorthLat.get(0).toString() + "," + westLonNorthLat.get(1).toString());

          List<String> categoriesList = new ArrayList<>();
          for (JsonElement individualCategory : categoriesArray) {
            categoriesList.add(individualCategory.getAsJsonObject().toString());
          }

          List<String> sourcesList = new ArrayList<>();
          for (JsonElement individualSource : sourcesArray) {
            sourcesList.add(individualSource.getAsJsonObject().toString());
          }

          CoordinateGeometry coordGeo = new CoordinateGeometry();
          coordGeo.setId(eventId.getAsString());
          coordGeo.setTitle(eventTitle.getAsString());
          coordGeo.setDescription(eventDescription.getAsString());
          coordGeo.setLink(eventLink.getAsString());
          if (eventClosedDate != null) {
            coordGeo.setClosedDate(eventClosedDate.getAsString());
          } else {
            coordGeo.setClosedDate("");
          }
          coordGeo.setCategories(categoriesList);
          coordGeo.setSources(sourcesList);
          coordGeo.setDate(geometryDate);
          coordGeo.setType(geometryType);
          coordGeo.setCoordinates(geometryCoordinatesList);
          coordGeometryList.add(coordGeo);
        } else {
          //must be Polygon
        }
      }
    }
    //for each CoordinateGeometry 
    for(CoordinateGeometry geo: coordGeometryList) {
      //correlate the temporal presence
      String painlessTemporalScript = 
              "def startDate = new Date(doc['start_date'][0]); "
                      + "def endDate = new Date(doc['end_date'][0]); "
                      + "def sf = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\"); "
                      + "def eventDate = sf.parse(\"" + geo.getDate() + "\"); "
                      + "if(eventDate.after(startDate) && eventDate.before(endDate)) { "
                      + "return true; }";
      JsonObject resultsJSON = null;
      BoolQueryBuilder queryBuilder = null;
      //and the spatial presence depending on whether it is a single Point; 
      //where the "coordinates" member must be a single position, 
      //multiple Point's; again where each "coordinates" member must be a single 
      //position (not to be confused with 'MultiPoint' where the the 
      //"coordinates" member must be an array of positions.), or 'Polygon' where
      // the "coordinates" member must be an array of LinearRing coordinate arrays.
      if("Point".equals(geo.type)) {
        try {
          double lon = Double.parseDouble(geo.getCoordinates().get(0).split(",")[0]);
          double lat = Double.parseDouble(geo.getCoordinates().get(0).split(",")[1]);
          //bypasses potential bugs (lon being lat and vice verse) which are sporadically 
          //peppered throughout the EONET dataset. Here we simply reverse the lon lat values if
          //values are known to be outside of the X and Y limits e.g. -180 to 180 and -90 to 90.
          if (lat < -90 || lat > 90) {
            double new_lon = lat;
            lat = lon;
            lon = new_lon;
          }
          queryBuilder = QueryBuilders.boolQuery()
                  .must(QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, "painless", painlessTemporalScript, new HashMap<>(), new HashMap<>())))
                  .must(QueryBuilders.geoShapeQuery(
                          "location",
                          ShapeBuilders.newPoint(new Coordinate(lon, lat))).relation(ShapeRelation.CONTAINS));
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        //assume Polygon
      }
      if (queryBuilder != null) {
        resultsJSON = (JsonObject) new JsonParser().parse(esDriver.searchByQuery(esIndex, queryBuilder, false, type));
      }
      esDriver.createBulkProcessor();
      if (resultsJSON != null) {
        BulkProcessor bulkProcessor = esDriver.getBulkProcessor();
        LOG.info("Retrieved {} hits from event-collection spatio-temporal correlation query.", resultsJSON.get("PDResults").getAsJsonArray().size());
        for (JsonElement result: resultsJSON.get("PDResults").getAsJsonArray()) {
          JsonObject jsonResult = result.getAsJsonObject();
          String id = jsonResult.get("id").getAsString();
          XContentBuilder eventMapping = null;
          try {
            eventMapping = jsonBuilder()
                    .startObject()
                    .field("id", geo.getId())
                    .field("title", geo.getTitle())
                    .field("description", geo.getDescription())
                    .field("link", geo.getLink())
                    .field("closed", geo.getClosedDate())
                    .array("categories", geo.getCategories())
                    .array("sources", geo.getSources())
                    .array("coordinates", geo.getCoordinates())
                    .field("date", geo.getDate())
                    .field("type", geo.getType())
                    .endObject();
          } catch (IOException e) {
            e.printStackTrace();
          }

          //events and Dataset-Metadata both exist in the result so we can script an update for both
          if (jsonResult.get("events") != null && jsonResult.get("Dataset-Metadata") != null) {
            String updateScript = 
                    "if (!ctx._source[\"Dataset-Metadata\"].contains(params.title)) { "
                            + "ctx._source[\"Dataset-Metadata\"].add(params.title); "
                            + "} "
                            + "if (!ctx._source.events.contains(params.event)) { "
                            + "ctx._source.events.add(params.event); "
                            + "}";
            Map<String, Object> params = new HashMap<>();
            if (!"".equals(geo.getTitle())) {
              params.put("title", geo.getTitle());
            }
            try {
              Map<? ,?> jsonMap = new ObjectMapper().readValue(eventMapping.string(), HashMap.class);
              params.put("event", jsonMap);
            } catch (IOException e1) {
              e1.printStackTrace();
            }

            UpdateRequest updateRequest = new UpdateRequest(esIndex, type, id)
                    .script(new Script(ScriptType.INLINE, "painless", updateScript, new HashMap<>(), params));
            bulkProcessor.add(updateRequest);
            //events exist but Dataset-Metadata does not so we can script an update for the former 
            //and an upsert for the latter
          } else if (jsonResult.get("events") != null && jsonResult.get("Dataset-Metadata") == null){
            String updateScript = 
                    "if (!ctx._source.events.contains(params.event)) { "
                            + "ctx._source.events.add(params.event); "
                            + "}";
            Map<String, Object> params = new HashMap<>();
            try {
              Map<? ,?> jsonMap = new ObjectMapper().readValue(eventMapping.string(), HashMap.class);
              params.put("event", jsonMap);
            } catch (IOException e) {
              e.printStackTrace();
            }
            UpdateRequest scriptUpdateRequest = new UpdateRequest(esIndex, type, id)
                    .script(new Script(ScriptType.INLINE, "painless", updateScript, new HashMap<>(), params));
            bulkProcessor.add(scriptUpdateRequest);
            IndexRequest indexRequest = null;
            XContentBuilder mapping = null;
            try {
              mapping = jsonBuilder()
                      .startObject()
                      .array("Dataset-Metadata", geo.getTitle())
                      .endObject();
            } catch (IOException e) {
              e.printStackTrace();
            }
            indexRequest = new IndexRequest(
                    esIndex, type, id).source(mapping);
            UpdateRequest upsertUpdateRequest =
                    new UpdateRequest(esIndex, type, id).upsert(indexRequest);
            upsertUpdateRequest.doc(indexRequest);
            bulkProcessor.add(upsertUpdateRequest);
            //events do not exist but Dataset-Metadata does so we can script an update for the latter 
            //and an upsert for the former
          } else if (jsonResult.get("events") == null && jsonResult.get("Dataset-Metadata") != null){
            String updateScript = 
                    "if (!ctx._source[\"Dataset-Metadata\"].contains(params.title)) { "
                            + "ctx._source[\"Dataset-Metadata\"].add(params.title); "
                            + "}";
            Map<String, Object> params = new HashMap<>();
            if (!"".equals(geo.getTitle())) {
              params.put("title", geo.getTitle());
            }
            UpdateRequest scriptUpdateRequest = new UpdateRequest(esIndex, type, id)
                    .script(new Script(ScriptType.INLINE, "painless", updateScript, new HashMap<>(), params));
            bulkProcessor.add(scriptUpdateRequest);
            IndexRequest indexRequest = null;
            XContentBuilder mapping = null;
            try {
              mapping = jsonBuilder()
                      .startObject()
                      .startArray("events")
                      .startObject()
                      .field("id", geo.getId())
                      .field("title", geo.getTitle())
                      .field("description", geo.getDescription())
                      .field("link", geo.getLink())
                      .field("closed", geo.getClosedDate())
                      .array("categories", geo.getCategories())
                      .array("sources", geo.getSources())
                      .array("coordinates", geo.getCoordinates())
                      .field("date", geo.getDate())
                      .field("type", geo.getType())
                      .endObject()
                      .endArray()
                      .endObject();
            } catch (IOException e1) {
              e1.printStackTrace();
            }
            indexRequest = new IndexRequest(
                    esIndex, type, id).source(mapping);
            UpdateRequest upsertUpdateRequest =
                    new UpdateRequest(esIndex, type, id).upsert(indexRequest);
            upsertUpdateRequest.doc(indexRequest);
            bulkProcessor.add(upsertUpdateRequest);
            //neither events or Dataset-Metadata exists, we need to create an upsert for both new fields.
          } else {
            IndexRequest indexRequest = null;
            try {
              XContentBuilder mapping = jsonBuilder()
                      .startObject()
                      .startArray("events")
                      .startObject()
                      .field("id", geo.getId())
                      .field("title", geo.getTitle())
                      .field("description", geo.getDescription())
                      .field("link", geo.getLink())
                      .field("closed", geo.getClosedDate())
                      .array("categories", geo.getCategories())
                      .array("sources", geo.getSources())
                      .array("coordinates", geo.getCoordinates())
                      .field("date", geo.getDate())
                      .field("type", geo.getType())
                      .endObject()
                      .endArray()
                      .array("Dataset-Metadata", geo.getTitle())
                      .endObject();
              indexRequest = new IndexRequest(
                      esIndex, type, id).source(mapping);
            } catch (IOException e) {
              e.printStackTrace();
            }
            UpdateRequest updateRequest =
                    new UpdateRequest(esIndex, type, id).upsert(indexRequest);
            updateRequest.doc(indexRequest);
            bulkProcessor.add(updateRequest);
          }
        }
        bulkProcessor.flush();
        esDriver.refreshIndex();
      }
      esDriver.destroyBulkProcessor();
    }
  }

  private class CoordinateGeometry {
    private String date;
    private String type;
    private List<String> coordinates;
    private String title;
    private List<String> sources;
    private List<String> categories;
    private String closedDate;
    private String link;
    private String description;
    private String id;

    /**
     * @return the date
     */
    public String getDate() {
      return date;
    }

    public List<String> getSources() {
      return sources;
    }

    public List<String> getCategories() {
      return categories;
    }

    public String getClosedDate() {
      return closedDate;
    }

    public String getLink() {
      return link;
    }

    public String getDescription() {
      return description;
    }

    public String getId() {
      return id;
    }

    public void setSources(List<String> sourcesList) {
      this.sources = sourcesList;
    }

    public void setCategories(List<String> categoriesList) {
      this.categories = categoriesList;
    }

    public void setClosedDate(String asString) {
      this.closedDate = asString;
    }

    public void setLink(String asString) {
      this.link = asString;
    }

    public void setDescription(String asString) {
      this.description = asString;
    }

    public void setId(String asString) {
      this.id = asString;
    }

    public void setTitle(String asString) {
      this.title = asString;

    }

    public String getTitle() {
      return title;
    }

    /**
     * @param date the date to set
     */
    public void setDate(String date) {
      this.date = date;
    }

    /**
     * @return the type
     */
    public String getType() {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * @return the coordinates
     */
    public List<String> getCoordinates() {
      return coordinates;
    }

    /**
     * @param geometryCoordinatesList the coordinates to set
     */
    public void setCoordinates(List<String> geometryCoordinatesList) {
      this.coordinates = geometryCoordinatesList;
    }

  }

  private static MudrodEngine checkConfigInit(MudrodEngine mudrodEngine) {
    if (mudrodEngine.getConfig().isEmpty()) {
      mudrodEngine.loadConfig();
      mudrodEngine.setESDriver(mudrodEngine.startESDriver());
    }
    return mudrodEngine;

  }

  public String ingestAllEonetEvents(MudrodEngine mEngine) {
    ESDriver esDriver = mEngine.getESDriver();
    if (mEngine.getConfig().isEmpty()) {
      mEngine.loadConfig();
      esDriver = mEngine.startESDriver();
    }
    String result = null;
    try {
      result = executeBulkIndexRequest(mEngine, esDriver, executeEonetGetOperations());
    } catch (SSLHandshakeException e) {
      e.printStackTrace();
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
    @SuppressWarnings("unused")
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
        e.printStackTrace();
      }
      if (updateResponse != null) {
        result = updateResponse.getGetResult();
      }
    }
    esDriver.destroyBulkProcessor();
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
        e.printStackTrace();
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
        e.printStackTrace();
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
      request.addHeader("User-Agent", "Apache SDAP Search Event Ingester");
      LOG.info("Executing: {}", request.toString());
      try {
        response = client.execute(request);
        HttpEntity entity = response.getEntity();
        JsonArray partialEventsArray = extractEventsArrayFromJsonResponse(EntityUtils.toString(entity, "UTF-8"));
        for (JsonElement jsonElement : partialEventsArray) {
          eventArray.add(jsonElement);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return eventArray;
  }

  private JsonArray extractEventsArrayFromJsonResponse(String string) {
    return ((JsonObject) new JsonParser().parse(string)).getAsJsonArray("events");
  }

  public String ingestEventsJSON(MudrodEngine mEngine, InputStream is) {
    return null;
  }

}
