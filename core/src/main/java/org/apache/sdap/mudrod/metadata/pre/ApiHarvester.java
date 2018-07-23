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
package org.apache.sdap.mudrod.metadata.pre;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.io.IOUtils;
import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.utils.HttpRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Properties;

/**
 * Harvest metadata from PO.DAAC Webservices.
 */
public class ApiHarvester extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ApiHarvester.class);

  /**
   * Creates a new instance of ApiHarvester.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch drive
   * @param spark the spark driver
   */
  public ApiHarvester(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    initMudrod("RawMetadata");
  }

  @Override
  public Object execute() {
    LOG.info("Starting Metadata harvesting.");
    startTime = System.currentTimeMillis();
    //remove old metadata from ES
    es.deleteType(
            props.getProperty(MudrodConstants.ES_INDEX_NAME),
            props.getProperty(MudrodConstants.RAW_METADATA_TYPE));

    //harvest new metadata using PO.DAAC web services
    if("1".equals(props.getProperty(MudrodConstants.METADATA_DOWNLOAD)))
      harvestMetadatafromWeb();
    es.createBulkProcessor();
    importToES();
    es.destroyBulkProcessor();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Metadata harvesting completed. Time elapsed: {}", (endTime - startTime) / 1000);
    return null;
  }

  /**
   * Index metadata into elasticsearch from local file directory.
   * Please make sure metadata have been harvest from web service before
   * invoking this method.
   */
  private void importToES() {
    File directory = new File(props.getProperty(MudrodConstants.RAW_METADATA_PATH));
    if(!directory.exists())
      directory.mkdir();
    File[] fList = directory.listFiles();
    for (File file : fList) {
      InputStream is;
      try {
        is = new FileInputStream(file);
        importSingleFileToES(is);
      } catch (FileNotFoundException e) {
        LOG.error("Error finding file!", e);
      }

    }
  }

  private void importSingleFileToES(InputStream is) {
    try {
      String jsonTxt = IOUtils.toString(is);
      JsonObject item = new JsonParser().parse(jsonTxt).getAsJsonObject();

      //obtain bounding box features and add them as a new field
      float westLon = 0;
      float eastLon = 0;
      float northLat = 0;
      float southLat = 0;
      if (!"".equals(item.get("DatasetCoverage-WestLon").getAsString())) {
        westLon = Float.parseFloat(item.get("DatasetCoverage-WestLon").getAsString());
      }
      if (!"".equals(item.get("DatasetCoverage-EastLon").getAsString())) {
        eastLon = Float.parseFloat(item.get("DatasetCoverage-EastLon").getAsString());
      }
      if (!"".equals(item.get("DatasetCoverage-NorthLat").getAsString())) {
        northLat = Float.parseFloat(item.get("DatasetCoverage-NorthLat").getAsString());
      }
      if (!"".equals(item.get("DatasetCoverage-SouthLat").getAsString())) {
        southLat = Float.parseFloat(item.get("DatasetCoverage-SouthLat").getAsString());
      }
      //normalize lat lon values
      if (westLon < -180) {
        westLon = -180;
      } else {
        //need to convert from longitudes from the 0-360 format common in climate data
        //to the more standard -180 to +180 format.
        westLon = ((westLon + 180) / 360) - 180;
      }
      String normalizedWestLon = String.valueOf(westLon);
      String normalizedEastLon = String.valueOf((eastLon > 180) ? eastLon = 180 : eastLon);
      String normalizedNorthLat = String.valueOf((northLat > 90) ? northLat = 90 : northLat);
      String normalizedSouthLat = String.valueOf((southLat < -90) ? southLat = -90 : southLat);
      //add bounding box to JSON object as the 'envelope' Geo-shape, see
      //https://www.elastic.co/guide/en/elasticsearch/reference/6.2/geo-shape.html
      JsonElement envelopeJsonElement = new JsonParser().parse(
              "{\"type\" : \"envelope\", \"coordinates\" : [ [" 
                      + normalizedWestLon + ", " + normalizedNorthLat + "], [" + normalizedEastLon + ", " + normalizedSouthLat + "] ]}");
      item.add("location", envelopeJsonElement);

      //add new date fields for start_date, if 'Dataset-DatasetCoverage-StartTimeLong' is unreliable, try
      //'DatasetCoverage-StartTimeLong-Long' or 'DatasetCoverage-StartTimeLong'
      Long startDateLong = null;
      if (!"".equals(item.get("Dataset-DatasetCoverage-StartTimeLong").getAsString())) {
        startDateLong = item.get("Dataset-DatasetCoverage-StartTimeLong").getAsLong();
      }
      LocalDate startDate = Instant.ofEpochMilli(startDateLong).atZone(ZoneId.systemDefault()).toLocalDate();
      JsonElement startDateJsonElement = new JsonParser().parse(startDate.toString());
      item.add("start_date", startDateJsonElement);

      //add new date fields for stop_date, if 'Dataset-DatasetCoverage-StartTimeLong' is unreliable, try
      //'DatasetCoverage-StopTimeLong' or 'DatasetCoverage-StopTimeLong-Long'
      Long endDateLong = null;
      if ("".equals(item.get("Dataset-DatasetCoverage-StopTimeLong").getAsString())) {
        //This represents a fabricated time in the future, Monday, February 14, 2022 12:00:00 AM UTC
        //this date time is specified such so that we are able to execute date range queries for things
        //like event correlation. It should be noted that when interpreted by the WebUI this datetime is
        //mapped to 'now' indicating that the dataset coverage is ongoing.
        endDateLong = Long.parseLong("1644796800000");
      } else {
        endDateLong = item.get("Dataset-DatasetCoverage-StopTimeLong").getAsLong();
      }
      LocalDate endDate = Instant.ofEpochMilli(endDateLong).atZone(ZoneId.systemDefault()).toLocalDate();
      JsonElement endDateJsonElement = new JsonParser().parse(endDate.toString());
      item.add("end_date", endDateJsonElement);
      IndexRequest ir = new IndexRequest(
              props.getProperty(MudrodConstants.ES_INDEX_NAME),
              props.getProperty(MudrodConstants.RAW_METADATA_TYPE)).source(item.toString());
      es.getBulkProcessor().add(ir);
    } catch (IOException e) {
      LOG.error("Error indexing metadata record!", e);
    }
  }

  /**
   * Harvest metadata from PO.DAAC web service.
   */
  private void harvestMetadatafromWeb() {
    LOG.info("Metadata download started.");
    int startIndex = 0;
    int docLength = 0;
    JsonParser parser = new JsonParser();
    do {
      String searchAPI = props.getProperty(MudrodConstants.METADATA_DOWNLOAD_URL);
      searchAPI = searchAPI.replace("$startIndex", Integer.toString(startIndex));
      HttpRequest http = new HttpRequest();
      String response = http.getRequest(searchAPI);

      JsonElement json = parser.parse(response);
      JsonObject responseObject = json.getAsJsonObject();
      JsonArray docs = responseObject.getAsJsonObject("response").getAsJsonArray("docs");

      docLength = docs.size();

      File file = new File(props.getProperty(MudrodConstants.RAW_METADATA_PATH));
      if (!file.exists()) {
        if (file.mkdir()) {
          LOG.info("Directory is created!");
        } else {
          LOG.error("Failed to create directory!");
        }
      }
      for (int i = 0; i < docLength; i++) {
        JsonElement item = docs.get(i);
        int docId = startIndex + i;
        File itemfile = new File(props.getProperty(MudrodConstants.RAW_METADATA_PATH) + "/" + docId + ".json");

        try (FileWriter fw = new FileWriter(itemfile.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw)) {
          itemfile.createNewFile();
          bw.write(item.toString());
        } catch (IOException e) {
          LOG.error("Error writing metadata to local file!", e);
        }
      }

      startIndex += 10;

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Error entering Elasticsearch Mappings!", e);
        Thread.currentThread().interrupt();
      }

    } while (docLength != 0);

    LOG.info("Metadata downloading finished");
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * @param args provide path to target data directory you 
   * intend to harvest metadata to.
   */
  public static void main(String[] args) {
    MudrodEngine mEngine = new MudrodEngine();
    Properties props = mEngine.loadConfig();
    props.put(MudrodConstants.RAW_METADATA_PATH, args[0]);
    ApiHarvester apiHarvester = new ApiHarvester(props, mEngine.startESDriver(), null);
    apiHarvester.execute();
  }

}
