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
package org.apache.sdap.mudrod.recommendation.pre;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.utils.LabeledRowMatrix;
import org.apache.sdap.mudrod.utils.MatrixUtil;
import org.apache.sdap.mudrod.weblog.structure.session.SessionExtractor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: SessionCooccurenceMatrix Function: Generate metadata session
 * coocucurence matrix from web logs. Each row in the matrix is corresponding to
 * a metadata, and each column is a session.
 */
public class SessionCooccurence extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionCooccurence.class);

  /**
   * Creates a new instance of SessionCooccurence.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark driver
   */
  public SessionCooccurence(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("Starting dataset session-based similarity generation...");

    startTime = System.currentTimeMillis();

    // get all metadata session cooccurance data
    SessionExtractor extractor = new SessionExtractor();
    JavaPairRDD<String, List<String>> sessionDatasetRDD = extractor.bulidSessionDatasetRDD(props, es, spark);

    // remove retired datasets
    LabeledRowMatrix datasetSessionMatrix = MatrixUtil.createWordDocMatrix(sessionDatasetRDD);

    // export
    MatrixUtil.exportToCSV(datasetSessionMatrix.rowMatrix, datasetSessionMatrix.rowkeys, datasetSessionMatrix.colkeys, props.getProperty(MudrodConstants.METADATA_SESSION_MATRIX_PATH));

    endTime = System.currentTimeMillis();

    LOG.info("Completed dataset session-based  similarity generation. Time elapsed: {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * filter out-of-data metadata
   *
   * @param es
   *          the Elasticsearch drive
   * @param userDatasetsRDD
   *          dataset extracted from session
   * @return filtered session datasets
   */
  public JavaPairRDD<String, List<String>> removeRetiredDataset(ESDriver es, JavaPairRDD<String, List<String>> userDatasetsRDD) {

    Map<String, String> nameMap = this.getOnServiceMetadata(es);

    return userDatasetsRDD.mapToPair(new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, List<String>> call(Tuple2<String, List<String>> arg0) throws Exception {
        List<String> oriDatasets = arg0._2;
        List<String> newDatasets = new ArrayList<>();
        for (String name : oriDatasets) {
          if (nameMap.containsKey(name)) {
            newDatasets.add(nameMap.get(name));
          }
        }
        return new Tuple2<>(arg0._1, newDatasets);
      }
    });

  }

  /**
   * getMetadataNameMap: Get on service metadata names, key is lowcase of short
   * name and value is the original short name
   *
   * @param es
   *          the elasticsearch client
   * @return a map from lower case metadata name to original metadata name
   */
  private Map<String, String> getOnServiceMetadata(ESDriver es) {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    String metadataType = props.getProperty("recom_metadataType");

    Map<String, String> shortnameMap = new HashMap<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(metadataType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        String shortName = (String) metadata.get(props.getProperty(MudrodConstants.METADATA_ID));
        shortnameMap.put(shortName.toLowerCase(Locale.ROOT), shortName);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return shortnameMap;
  }

}
