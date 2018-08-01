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
package org.apache.sdap.mudrod.ranking.common;

import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.ranking.dlrank.DLRankLearner;
import org.apache.sdap.mudrod.ranking.ranksvm.RankSVMLearner;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.Properties;

/**
 * Create a learner due to configuration
 */
public class LearnerFactory extends MudrodAbstract {

  public LearnerFactory(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  public RankLearner createLearner() {
    /*if ("1".equals(props.getProperty(MudrodConstants.RANKING_ML)))
      return new SVMLearner(props, es, spark, props.getProperty(MudrodConstants.RANKING_MODEL));

    return null;*/
    return new RankSVMLearner(props, es, spark, props.getProperty(MudrodConstants.RANKING_MODEL));
    //return new DLRankLearner(props, es, spark, "");
  }
}
