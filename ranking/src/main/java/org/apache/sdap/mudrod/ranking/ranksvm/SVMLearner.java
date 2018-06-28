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
package org.apache.sdap.mudrod.ranking.ranksvm;

import java.io.File;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ranking.common.Learner;
import org.apache.sdap.mudrod.ranking.common.LearnerFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

/**
 * Supports the ability to importing classifier into memory
 */
public class SVMLearner extends Learner {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	SVMModel model = null;
	transient SparkContext sc = null;

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
	public SVMLearner(Properties props, ESDriver es, SparkDriver spark, String svmSgdModel) {
		super(props, es, spark);

		sc = spark.sc.sc();
		load(svmSgdModel);
	}

	public String prepareTrainData(String sourceDir) {
		
		//add other source, such as log, streaming
		String resultFile = this.extractTrainDataFromExperts(sourceDir);
		
		//String path = SVMLearner.class.getClassLoader().getResource("trainsets").toString();
		String path = new File(resultFile).getParent();
		String svmSparkFile = path + "/inputDataForSVM_spark.txt";
		SparkFormatter sf = new SparkFormatter();
		sf.toSparkSVMformat(resultFile,svmSparkFile);
		
		return svmSparkFile;
	}

	@Override
	public void train(String trainFile) {
		//String path = SVMLearner.class.getClassLoader().getResource("inputDataForSVM_spark.txt").toString();
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, trainFile).toJavaRDD();

		// Run training algorithm to build the model.
		int numIterations = 100;
		model = SVMWithSGD.train(data.rdd(), numIterations);
	}

	@Override
	public double predict(double[] value) {
		LabeledPoint p = new LabeledPoint(99.0, Vectors.dense(value));
		return model.predict(p.features());
	}

	@Override
	public void save() {
		// Save and load model
		String modelPath = SVMLearner.class.getClassLoader().getResource("javaSVMWithSGDModel").toString();
		model.save(sc, modelPath);
	}

	@Override
	public void load(String svmSgdModel) {
		// TODO Auto-generated method stub
		sc.addFile(svmSgdModel, true);
		model = SVMModel.load(sc, svmSgdModel);
	}

	public static void main(String[] arg0) {
		MudrodEngine me = new MudrodEngine();
		Properties props = me.loadConfig();

		SparkDriver spark = new SparkDriver(me.getConfig());
		ESDriver es = new ESDriver(me.getConfig());

		LearnerFactory factory = new LearnerFactory(props, es, spark);
		Learner le = factory.createLearner();
		
		String sourceDir = "E://data//mudrod//ranking//rankingResults//training//training_data_v4";
		String trainFile = le.prepareTrainData(sourceDir);
		le.train(trainFile);
		le.save();
	}
}
