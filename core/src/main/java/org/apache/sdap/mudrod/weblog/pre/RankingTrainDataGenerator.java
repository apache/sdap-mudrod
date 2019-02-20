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
package org.apache.sdap.mudrod.weblog.pre;

import org.apache.sdap.mudrod.discoveryengine.DiscoveryStepAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.weblog.structure.session.RankingTrainData;
import org.apache.sdap.mudrod.weblog.structure.session.SessionExtractor;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RankingTrainDataGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RankingTrainDataGenerator.class);

  public RankingTrainDataGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info("Starting generate ranking train data.");
    startTime = System.currentTimeMillis();

    String rankingTrainFile = "E:\\Mudrod_input_data\\Testing_Data_4_1monthLog+Meta+Onto\\traing.txt";
    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<RankingTrainData> rankingTrainDataRDD = extractor.extractRankingTrainData(this.props, this.es, this.spark);

      JavaRDD<String> rankingTrainData_JsonRDD = rankingTrainDataRDD.map(f -> f.toJson());

      rankingTrainData_JsonRDD.coalesce(1, true).saveAsTextFile(rankingTrainFile);

    } catch (Exception e) {
      LOG.error("Train data ranking failed : ", e);
    }

    endTime = System.currentTimeMillis();
    LOG.info("Ranking train data generation complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
