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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ranking.common.RankLearner;
import org.apache.sdap.mudrod.ranking.common.LearnerFactory;
import org.apache.sdap.mudrod.ranking.traindata.RankTrainDataFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

/**
 * Learn ranking weights with SVM model
 */
public class RankSVMLearner extends RankLearner {
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
   *          classifier type
   * @param skd
   *          an instance of spark driver
   * @param svmSgdModel
   *          path to a trained model
   */
  public RankSVMLearner(Properties props, ESDriver es, SparkDriver spark, String svmSgdModel) {
    super(props, es, spark);
    sc = spark.sc.sc();
    load(svmSgdModel);
  }

  @Override
  public String customizeData(String sourceDir, String outFileName) {
    RankTrainDataFactory factory = new RankTrainDataFactory(props, es, spark);
    String resultFile = factory.createRankTrainData("experts", sourceDir);

    String path = new File(resultFile).getParent();
    
    String separator = System.getProperty("file.separator");
    String svmSparkFile = path + separator + outFileName + ".txt";
    SparkFormatter sf = new SparkFormatter();
    sf.toSparkSVMformat(resultFile, svmSparkFile);

    return svmSparkFile;
  }

  @Override
  public void train(String trainFile) {
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, trainFile).toJavaRDD();
    // Run training algorithm to build the model.
    int numIterations = 100;
    model = SVMWithSGD.train(data.rdd(), numIterations);
  }
  
  @Override
  public void evaluate(String testFile) {
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, testFile).toJavaRDD();
    // Run training algorithm to build the model.
    JavaRDD<Tuple2<Object, Object>> scoreAndLabels = data.map(p->{
        double score = model.predict(p.features());
        return new Tuple2<>(score, p.label());
    });
    BinaryClassificationMetrics metrics =  new BinaryClassificationMetrics(scoreAndLabels.rdd());
    System.out.println("Area under ROC = " + metrics.areaUnderROC());
    long correctNum = scoreAndLabels.filter(new Function<Tuple2<Object,Object>, Boolean>(){
      @Override
      public Boolean call(Tuple2<Object, Object> arg0) throws Exception {
        Integer predict = (Integer)arg0._1();
        Integer label = (Integer)arg0._2();
        int output = 0;
        if (label == -1.0) {
          output = 0;
        } else if (label == 1.0) {
          output = 1;
        }
        
        if(predict == output){
          return true;
        }
        return false;
      }
    }).count();
    System.out.println("Accuracy = " + correctNum/scoreAndLabels.count());
  }

  @Override
  public double predict(double[] value) {
    LabeledPoint p = new LabeledPoint(99.0, Vectors.dense(value));
    return  model.predict(p.features());
  }

  @Override
  public void save() {
    // Save model
    String modelPath = RankSVMLearner.class.getClassLoader().getResource("javaSVMWithSGDModel").toString();
    model.save(sc, modelPath);
  }

  @Override
  public void load(String svmSgdModel) {
    // load model
    sc.addFile(svmSgdModel, true);
    model = SVMModel.load(sc, svmSgdModel);
  }

  public static void main(String[] arg0) {
    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();

    SparkDriver spark = new SparkDriver(props);
    ESDriver es = new ESDriver(props);

    LearnerFactory factory = new LearnerFactory(props, es, spark);
    RankLearner le = factory.createLearner();
    String sourceDir = arg0[0];
    String trainFile = le.customizeTrainData(sourceDir);
    le.train(trainFile);
    le.save();
  }
}
