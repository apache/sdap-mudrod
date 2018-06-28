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
import org.apache.sdap.mudrod.ranking.traindata.ExpertData;
import org.apache.sdap.mudrod.ranking.traindata.TrainingImporter;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Supports the ability to importing classifier into memory
 */
public abstract class Learner extends MudrodAbstract {

	public Learner(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Constructor to load in spark SVM classifier
	 *
	 * @param classifierName
	 *            classifier type
	 * @param skd
	 *            an instance of spark driver
	 * @param svmSgdModel
	 *            path to a trained model
	 */

	public String extractTrainDataFromExperts(String sourceDir){
	  File sourceFile = new File(sourceDir);
	  boolean bDir = sourceFile.isDirectory();
	  boolean multFiles = false;
	  if(bDir){
		  multFiles = true;
	  }
	  
	  String resultDir = sourceFile.getParent() + "/trainsets.txt";
	  ExpertData converter = new ExpertData(sourceDir, resultDir, true);
      converter.convert2TrainSet();
      
      return resultDir;
	}

	/**
	 * Method of classifying instance
	 *
	 * @param p
	 *            the instance that needs to be classified
	 * @return the class id
	 */
	//public abstract double classify(LabeledPoint p);

	public abstract String prepareTrainData(String sourceDir);

	public abstract void train(String trainFile);

	public abstract double predict(double[] value);
	
	public abstract void save();
	
	public abstract void load(String model);
}
