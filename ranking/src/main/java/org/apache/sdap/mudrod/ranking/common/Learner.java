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
import org.apache.sdap.mudrod.ranking.traindata.ExpertRankTrainData;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * learn rank weights from train data and predict search results ranking
 */
public abstract class Learner extends MudrodAbstract {

  public Learner(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
	}

	public abstract String customizeTrainData(String sourceDir);

	public abstract void train(String trainFile);

	public abstract double predict(double[] value);
	
	public abstract void save();
	
	public abstract void load(String model);
}
