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
package org.apache.sdap.mudrod.ontology.process;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to parse and process FTP and HTTP log files
 */
public class OntologyLinkCal extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyLinkCal.class);

  public OntologyLinkCal(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    es.deleteAllByQuery(props.getProperty(MudrodConstants.ES_INDEX_NAME), MudrodConstants.ONTOLOGY_LINKAGE_TYPE, QueryBuilders.matchAllQuery());
    addSWEETMapping();
  }

  /**
   * Method of adding mapping for triples extracted from SWEET
   */
  private void addSWEETMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(MudrodConstants.ONTOLOGY_LINKAGE_TYPE).startObject("properties").startObject("concept_A").field("type", "string")
          .field("index", "not_analyzed").endObject().startObject("concept_B").field("type", "string").field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(props.getProperty(MudrodConstants.ES_INDEX_NAME))
      .setType(MudrodConstants.ONTOLOGY_LINKAGE_TYPE).setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of calculating and importing SWEET triples into Elasticsearch
   */
  @Override
  public Object execute() {
    es.deleteType(props.getProperty(MudrodConstants.ES_INDEX_NAME), MudrodConstants.ONTOLOGY_LINKAGE_TYPE);
    es.createBulkProcessor();

    BufferedReader br = null;
    String line = "";
    double weight = 0;

    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(props.getProperty(MudrodConstants.ONTOLOGY_PATH)), StandardCharsets.UTF_8));
      while ((line = br.readLine()) != null) {
        String[] strList = line.toLowerCase(Locale.ENGLISH).split(",");
        if (strList[1].equals("subclassof")) {
          weight = 0.75;
        } else {
          weight = 0.9;
        }

        IndexRequest ir = new IndexRequest(props.getProperty(MudrodConstants.ES_INDEX_NAME), MudrodConstants.ONTOLOGY_LINKAGE_TYPE).source(
            jsonBuilder().startObject().field("concept_A", es.customAnalyzing(props.getProperty(MudrodConstants.ES_INDEX_NAME), strList[2]))
                .field("concept_B", es.customAnalyzing(props.getProperty(MudrodConstants.ES_INDEX_NAME), strList[0])).field("weight", weight).endObject());
        es.getBulkProcessor().add(ir);

      }

    } catch (IOException | ExecutionException | InterruptedException e) {
      LOG.error("Couldn't open file!", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    } finally {
      if (br != null) {
        try {
          br.close();
          es.destroyBulkProcessor();
          es.refreshIndex();
        } catch (IOException e) {
          LOG.error("Error whilst closing file!", e);
        }
      }
    }
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
