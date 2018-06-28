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
import org.apache.sdap.mudrod.ranking.ranksvm.SVMLearner;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.Properties;

/**
 * Supports the ability to importing classifier into memory
 */
public class LearnerFactory extends MudrodAbstract {

  public LearnerFactory(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
	}

/**
   * Method of classifying instance
   *
   * @param p the instance that needs to be classified
   * @return the class id
   */
  
  public Learner createLearner(){
	  if("1".equals(props.getProperty(MudrodConstants.RANKING_ML)))
	      return new SVMLearner(props, es, spark, props.getProperty(MudrodConstants.RANKING_MODEL));
	  
	  return null;
	  
  }
  

}
