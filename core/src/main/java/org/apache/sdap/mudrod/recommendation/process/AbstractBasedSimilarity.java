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
package org.apache.sdap.mudrod.recommendation.process;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.semantics.SVDAnalyzer;
import org.apache.sdap.mudrod.utils.LinkageTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * ClassName: Recommend metedata based on data content semantic similarity
 */
public class AbstractBasedSimilarity extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBasedSimilarity.class);

  /**
   * Creates a new instance of TopicBasedCF.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch client
   * @param spark the spark drive
   */
  public AbstractBasedSimilarity(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("Abstract similarity calculation starts.");
    startTime = System.currentTimeMillis();

    try {
      // for comparison
      SVDAnalyzer svd = new SVDAnalyzer(props, es, spark);
      svd.getSVDMatrix(props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH), 150, props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));
      List<LinkageTriple> tripleList = svd.calTermSimfromMatrix(props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));
      svd.saveToES(tripleList, props.getProperty(MudrodConstants.ES_INDEX_NAME), MudrodConstants.METADATA_WORD_SIM_TYPE, true, true);

    } catch (Exception e) {
      LOG.error("Abstract similarity calculation failed: ", e);
    }

    endTime = System.currentTimeMillis();
    LOG.info("Abstract similarity calculation ends. Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}
