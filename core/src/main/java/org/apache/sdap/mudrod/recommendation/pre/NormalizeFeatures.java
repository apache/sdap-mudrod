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
/**
 * This package includes the preprocessing, processing, and data structure used
 * by recommendation module.
 */
package org.apache.sdap.mudrod.recommendation.pre;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.recommendation.structure.MetadataFeature;
import org.apache.sdap.mudrod.recommendation.structure.PODAACMetadataFeature;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NormalizeFeatures extends DiscoveryStepAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(NormalizeFeatures.class);
  // index name
  private String indexName;
  // type name of metadata in ES
  private String metadataType;

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props the Mudrod configuration
   * @param es    an instantiated {@link ESDriver}
   * @param spark an instantiated {@link SparkDriver}
   */
  public NormalizeFeatures(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    metadataType = MudrodConstants.RECOM_METADATA_TYPE;
  }

  @Override
  public Object execute() {
    LOG.info("Preprocessing metadata feature starts.");
    startTime = System.currentTimeMillis();

    normalizeMetadataVariables(es);

    endTime = System.currentTimeMillis();
    LOG.info("Preprocessing metadata feature ends. Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public void normalizeMetadataVariables(ESDriver es) {

    es.createBulkProcessor();

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(metadataType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> updatedValues = new HashMap<>();

        //!!!important change to other normalizer class when using other metadata
        MetadataFeature normalizer = new PODAACMetadataFeature();
        normalizer.normalizeMetadataVariables(metadata, updatedValues);

        UpdateRequest ur = es.generateUpdateRequest(indexName, metadataType, hit.getId(), updatedValues);
        es.getBulkProcessor().add(ur);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }
}
