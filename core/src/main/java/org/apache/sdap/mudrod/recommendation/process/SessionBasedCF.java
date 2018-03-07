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
import org.apache.sdap.mudrod.semantics.SemanticAnalyzer;
import org.apache.sdap.mudrod.utils.LinkageTriple;
import org.apache.sdap.mudrod.utils.SimilarityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: Recommend metedata based on session level co-occurrence
 */
public class SessionBasedCF extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(SessionBasedCF.class);

  /**
   * Creates a new instance of SessionBasedCF.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public SessionBasedCF(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Session based metadata similarity starts******************");
    startTime = System.currentTimeMillis();

    try {
      String session_metadatFile = props.getProperty("session_metadata_Matrix");
      File f = new File(session_metadatFile);
      if (f.exists()) {
        SemanticAnalyzer analyzer = new SemanticAnalyzer(props, es, spark);
        List<LinkageTriple> triples = analyzer.calTermSimfromMatrix(session_metadatFile, SimilarityUtil.SIM_PEARSON, 1);
        analyzer.saveToES(triples, props.getProperty("indexName"), props.getProperty("metadataSessionBasedSimType"), true, false);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************Session based metadata similarity ends******************Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
